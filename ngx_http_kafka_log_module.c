/* Apacher kafka (librdkafka) log module
 * File: ngx_http_kafka_log_module.c
 * Symbol: klm
 * Started: Tue Apr 13 08:11:53 MSK 2021
 * Author: Max Amzarakov (maxam18 _at_ gmail _._ com)
 * Copyright (c) 2021 ..."
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <nginx.h>

#include "librdkafka/rdkafka.h"


#define NGX_HTTP_KLM_VERSION              "0.0.4"
#define NGX_HTTP_KLM_FILE_BUF_SIZE        4096
#define NGX_HTTP_KLM_FILE_BUF_SIZE_MAX    4096 * 100

/*
#define dd(...) fprintf( stderr, "KAFKA " __VA_ARGS__)
*/
#define dd(...)

typedef struct {
    ngx_str_t                    name;
    ngx_http_complex_value_t     fmt;
    ngx_http_complex_value_t     key;
} ngx_http_klm_fmt_t;

typedef struct {
    ngx_str_t                    name;
    rd_kafka_topic_t            *rkt;
    rd_kafka_t                  *rk;
} ngx_http_klm_topic_t;

typedef struct {
    ngx_str_t                    name;
    ngx_str_t                    value;
} ngx_http_klm_prop_t;

typedef struct ngx_http_klm_list_s ngx_http_klm_list_t;
struct ngx_http_klm_list_s {
    void                        *item;
    ngx_http_klm_list_t         *next;
};

typedef struct {
    ngx_str_t                    brokers;
    rd_kafka_t                  *rk;
    ngx_array_t                 *props;        /* ngx_http_klm_prop_t array  */
    ngx_array_t                 *formats;      /* ngx_http_klm_fmt_t array   */
    ngx_http_klm_list_t         *files;        /* files list                 */
    ngx_http_klm_list_t         *topics;       /* topics list                */
} ngx_http_klm_main_conf_t;

typedef struct {
    ngx_open_file_t             *open_file;
    time_t                       disk_full_time;
    time_t                       error_log_time;
    u_char                      *start;
    u_char                      *pos;
    u_char                      *end;
    ngx_str_t                    name;
} ngx_http_klm_file_t;

typedef struct {
    ngx_int_t                    partition;    /* log partition              */
    ngx_http_klm_file_t         *file;         /* fallback file              */
    ngx_http_klm_fmt_t          *format;       /* format pointer             */
    ngx_http_klm_topic_t        *topic;        /* topic pointer              */
} ngx_http_klm_log_t;

typedef struct {
    ngx_array_t                 *logs;    
} ngx_http_klm_loc_conf_t;

/* config */
static void *ngx_http_klm_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_klm_merge_loc_conf(ngx_conf_t *cf, void *parent
        , void *child);
static void *ngx_http_klm_create_main_conf(ngx_conf_t *cf);

static char *ngx_http_klm_conf_set_format(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_klm_conf_set_prop(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_klm_conf_set_log(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static ngx_int_t ngx_http_klm_init(ngx_conf_t *cf);
static ngx_int_t ngx_http_klm_init_worker(ngx_cycle_t *cycle);

static void ngx_http_klm_kafka_destroy(void *user);

ngx_int_t ngx_http_klm_handler(ngx_http_request_t *r);

static int ngx_http_klm_err_rate_pass();
static void ngx_http_klm_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
static void ngx_http_klm_log_cb(const rd_kafka_t *rk, int level, const char *fac, const char *buf);

static void ngx_http_klm_fb_flush(ngx_open_file_t *file, ngx_log_t *errlog);
static void ngx_http_klm_log_file(ngx_http_klm_file_t *file, ngx_str_t *msg, ngx_str_t *key);

static ngx_command_t ngx_http_klm_module_commands[] = {
    {   ngx_string("klm_brokers"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_klm_main_conf_t, brokers),
        NULL },
    {   ngx_string("klm_format"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE23,
        ngx_http_klm_conf_set_format,
        NGX_HTTP_MAIN_CONF_OFFSET,
        0,
        NULL },
    {   ngx_string("klm_prop"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE2,
        ngx_http_klm_conf_set_prop,
        NGX_HTTP_MAIN_CONF_OFFSET,
        0,
        NULL },
    {   ngx_string("klm_log"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE23,
        ngx_http_klm_conf_set_log,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL },

    ngx_null_command
};

static ngx_http_module_t ngx_http_klm_module_ctx = {
    NULL,                                   /* preconfiguration */
    ngx_http_klm_init,                      /* postconfiguration */
    ngx_http_klm_create_main_conf,          /* create main configuration */
    NULL,                                   /* init main configuration */
    NULL,                                   /* create server configuration */
    NULL,                                   /* merge server configuration */
    ngx_http_klm_create_loc_conf,           /* create location configration */
    ngx_http_klm_merge_loc_conf             /* merge location configration */
};

ngx_module_t ngx_http_kafka_log_module = {
    NGX_MODULE_V1,
    &ngx_http_klm_module_ctx,               /* module context */
    ngx_http_klm_module_commands,           /* module directives */
    NGX_HTTP_MODULE,                        /* module type */
    NULL,                                   /* init master */
    NULL,                                   /* init module */
    ngx_http_klm_init_worker,               /* init process */
    NULL,                                   /* init thread */
    NULL,                                   /* exit thread */
    NULL,                                   /* exit process */
    NULL,                                   /* exit master */
    NGX_MODULE_V1_PADDING
};

ngx_int_t ngx_http_klm_handler(ngx_http_request_t *r)
{
    ngx_http_klm_loc_conf_t     *lcf;
    ngx_http_klm_log_t          *log;
    ngx_str_t                    key, msg; 
    ngx_uint_t                   i;
    int                          err;

    lcf = ngx_http_get_module_loc_conf(r, ngx_http_kafka_log_module);

    if( lcf->logs == NGX_CONF_UNSET_PTR )
        return NGX_OK;

    log = lcf->logs->elts;
    for( i = 0; i < lcf->logs->nelts; ++i, ++log )
    {
        if( (ngx_http_complex_value(r, &log->format->fmt, &msg) != NGX_OK)
            || (ngx_http_complex_value(r, &log->format->key, &key) != NGX_OK) )
            continue;
        
        err = rd_kafka_produce(log->topic->rkt, log->partition
                        , RD_KAFKA_MSG_F_COPY
                        , msg.data, msg.len
                        , (const char *) key.data, key.len
                        , log
                    );
        if( err < 0 )
        {
            if( log->file )
                ngx_http_klm_log_file(log->file, &msg, &key);

            if( ngx_http_klm_err_rate_pass() )
            {
                ngx_log_error(NGX_LOG_ERR, r->pool->log, 0
                    , "kafka prod failed, topic: '%s', partition: %i, err: %s"
                    , rd_kafka_topic_name(log->topic->rkt), log->partition
                    , rd_kafka_err2str(rd_kafka_last_error()));
            }
        }

        ngx_log_debug3(NGX_LOG_DEBUG_HTTP, r->pool->log, 0
            , "kafka msg: '%V', key:'%V', err: %d "
            , &msg, &key, err);

        rd_kafka_poll(log->topic->rk, 0);
    }

    return NGX_OK;
}


static char *ngx_http_klm_conf_set_prop(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_klm_main_conf_t      *mcf = conf;
    ngx_http_klm_prop_t           *prop;
    ngx_str_t                     *args = cf->args->elts;

    prop = ngx_array_push(mcf->props);
    if (prop == NULL) {
        return NGX_CONF_ERROR;
    }

    ngx_memzero(prop, sizeof(*prop));

    prop->name  = args[1];
    prop->value = args[2];

    return NGX_CONF_OK;
}


static char *ngx_http_klm_conf_set_format(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_compile_complex_value_t   ccv;
    ngx_http_klm_main_conf_t          *mcf = conf;
    ngx_http_klm_fmt_t                *fmt;
    ngx_str_t                         *args = cf->args->elts;
    ngx_uint_t                         i;

    fmt = mcf->formats->elts;
    for (i = 0; i < mcf->formats->nelts; ++i, ++fmt)
        if (fmt->name.len == args[1].len
            && ngx_strncmp(fmt->name.data, args[1].data, args[1].len) == 0)
        {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "duplicate kafka log format name \"%V\"",
                               &args[1]);
            return NGX_CONF_ERROR;
        }

    if( i >= mcf->formats->nelts )
        if( (fmt = ngx_array_push(mcf->formats)) == NULL )
            return NGX_CONF_ERROR;

    fmt->name = args[1];

    ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

    ccv.cf = cf;
    ccv.value = &args[2];
    ccv.complex_value = &fmt->fmt;
    if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    if( cf->args->nelts > 3 )
    {
	ngx_memzero(&ccv, sizeof(ngx_http_compile_complex_value_t));

        ccv.cf = cf;
        ccv.value = &args[3];
        ccv.complex_value = &fmt->key;
        if( ngx_http_compile_complex_value(&ccv) != NGX_OK )
            return NGX_CONF_ERROR;
    } else 
    {
        ngx_memzero(&fmt->key, sizeof(fmt->key));
    }

    return NGX_CONF_OK;
}


static char *ngx_http_klm_conf_set_log(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_klm_main_conf_t    *mcf;
    ngx_http_klm_loc_conf_t     *lcf = conf;
    ngx_http_klm_list_t         *list;
    ngx_http_klm_log_t          *log;
    ngx_str_t                   *args = cf->args->elts;
    ngx_str_t                    name;
    ngx_uint_t                   i;
    u_char                      *p;

    mcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_kafka_log_module);

    if( lcf->logs == NGX_CONF_UNSET_PTR )
    {
        lcf->logs = ngx_array_create(cf->pool, 2, sizeof(ngx_http_klm_log_t));
        if (lcf->logs == NULL) {
            return NGX_CONF_ERROR;
        }
    }

    log = ngx_array_push(lcf->logs);
    if( log == NULL )
        return NGX_CONF_ERROR;

    if( (p = (u_char *)ngx_strchr(args[1].data, ':')) )
    {
        name.len = p - args[1].data;
        name.data = args[1].data;
        
        *p++ = 0;

        log->partition = ngx_atoi(p, args[1].len - (p-args[1].data));
    } else {
        name = args[1];
        log->partition = RD_KAFKA_PARTITION_UA;
    }

    for( list = mcf->topics;
         (log->topic = list->item) != NULL;
         list = list->next )
    {
        if( log->topic->name.len == name.len
             && ngx_strncmp(log->topic->name.data
                    , name.data, name.len) 
            == 0 )
            break;
    }

    if( log->topic == NULL )
    {
        if( (list->next = ngx_pcalloc(cf->pool, sizeof(*list->next))) == NULL )
            return NGX_CONF_ERROR;
        
        log->topic = list->item = ngx_pcalloc(cf->pool, sizeof(*log->topic));
        if( log->topic == NULL )
            return NGX_CONF_ERROR;

        log->topic->name = name;
    }

    log->format = mcf->formats->elts;
    for (i = 0; i < mcf->formats->nelts; ++i, ++log->format)
        if( log->format->name.len == args[2].len
            && ngx_strncmp(log->format->name.data
                            , args[2].data, args[2].len) 
            == 0 )
            break;

    if( i >= mcf->formats->nelts )
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                               "kafka log format name \"%V\" not found",
                               &args[2]);
        return NGX_CONF_ERROR;
    }

    if( cf->args->nelts == 3 )
        return NGX_CONF_OK;
    
    for( list = mcf->files; 
         (log->file = list->item) != NULL;
         list = list->next )
    {
        if( log->file->name.len == args[3].len
             && ngx_strncmp(log->file->name.data
                    , args[3].data, args[3].len) 
            == 0 )
            break;
    }

    if( log->file == NULL )
    {
        if( (list->next = ngx_pcalloc(cf->pool, sizeof(*list->next))) == NULL )
            return NGX_CONF_ERROR;

        log->file = list->item = ngx_pcalloc(cf->pool, sizeof(*log->file));
        if( log->file == NULL)
            return NGX_CONF_ERROR;

        log->file->name = args[3];
    }

    return NGX_CONF_OK;
}

static void *ngx_http_klm_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_klm_loc_conf_t     *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_klm_loc_conf_t));
    if( NULL == conf )
        return NGX_CONF_ERROR;

    conf->logs = NGX_CONF_UNSET_PTR;

    return conf;
}

static char *ngx_http_klm_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_klm_loc_conf_t     *prev = parent;
    ngx_http_klm_loc_conf_t     *conf = child;
    
    ngx_conf_merge_ptr_value(conf->logs, prev->logs, NGX_CONF_UNSET_PTR);

    return NGX_CONF_OK;
}

static void *ngx_http_klm_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_klm_main_conf_t *mcf;

    mcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_klm_main_conf_t));
    if( NULL == mcf )
        return NULL;
    
    mcf->topics = ngx_pcalloc(cf->pool, sizeof(*mcf->topics));
    if (mcf->topics == NULL)
        return NGX_CONF_ERROR;

    mcf->formats = ngx_array_create(cf->pool, 2, sizeof(ngx_http_klm_fmt_t));
    if (mcf->formats == NULL)
        return NGX_CONF_ERROR;
    
    mcf->props = ngx_array_create(cf->pool, 2, sizeof(ngx_http_klm_prop_t));
    if (mcf->props == NULL)
        return NGX_CONF_ERROR;

    mcf->files = ngx_pcalloc(cf->pool, sizeof(*mcf->files));
    if (mcf->files == NULL)
        return NGX_CONF_ERROR;

    return mcf;
}

static ngx_int_t ngx_http_klm_init(ngx_conf_t *cf)
{
    ngx_http_core_main_conf_t   *cmcf;
    ngx_http_klm_main_conf_t    *mcf;
    ngx_http_klm_list_t         *list;
    ngx_http_handler_pt         *hpt;
    ngx_http_klm_file_t         *file;

    mcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_kafka_log_module);
    if( mcf->brokers.len == 0 )
    {
        ngx_conf_log_error(NGX_LOG_WARN, cf, 0, "brokers are not set");
        return NGX_OK;
    }

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    hpt = ngx_array_push(&cmcf->phases[NGX_HTTP_LOG_PHASE].handlers);
    if( NULL == hpt )
        return NGX_ERROR;

    *hpt = ngx_http_klm_handler;


    for( list = mcf->files;
         (file = list->item) != NULL;
         list = list->next )
    {
        file->open_file = ngx_conf_open_file(cf->cycle, &file->name);
        if( NULL == file->open_file )
            return NGX_ERROR;
        
        file->open_file->data = file;
        file->open_file->flush = ngx_http_klm_fb_flush;

        file->start = ngx_palloc(cf->cycle->pool, NGX_HTTP_KLM_FILE_BUF_SIZE);
        if( NULL == file->start )
            return NGX_ERROR;
        
        file->pos = file->start;
        file->end = file->start + NGX_HTTP_KLM_FILE_BUF_SIZE;
    }

    return NGX_OK;
}

static ngx_int_t ngx_http_klm_init_worker(ngx_cycle_t *cycle)
{
    ngx_http_klm_main_conf_t      *mcf;
    ngx_http_klm_topic_t          *topic;
    ngx_http_klm_prop_t           *prop;
    ngx_http_klm_list_t           *list;
    ngx_pool_cleanup_t            *cln;

    rd_kafka_topic_conf_t         *rktc;
    rd_kafka_conf_res_t            res;
    rd_kafka_conf_t               *rkc;

    ngx_uint_t                     i;
    char                           errstr[2048];

    mcf = ngx_http_cycle_get_module_main_conf(cycle
                    , ngx_http_kafka_log_module);
    
    rkc = rd_kafka_conf_new();
    if( rkc == NULL )
    {
        ngx_log_error(NGX_LOG_ERR, cycle->pool->log, 0,
                "kafka_log: rd_kafka_conf_new failed");
        return NGX_ERROR;
    }

    if (rd_kafka_conf_set(rkc, "bootstrap.servers"
                             , (const char *) mcf->brokers.data
                            , errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        ngx_log_error(NGX_LOG_ERR, cycle->pool->log, 0, "%s", errstr);
        rd_kafka_conf_destroy(rkc);
        return 1;
    }

    prop = mcf->props->elts;
    for( i = 0; i < mcf->props->nelts; ++i, ++prop )
    {
        res = rd_kafka_conf_set(rkc, (char *)prop->name.data, (char *)prop->value.data
                                   , errstr, sizeof(errstr));
        if( res != RD_KAFKA_CONF_OK )
        {
            ngx_log_error(NGX_LOG_ERR, cycle->pool->log, 0,
                  "kafka_log: rd_kafka_conf_set failed [%V] => [%V] : %s"
                , &prop->name, &prop->value, errstr);
            return NGX_ERROR;
        }
    }

    rd_kafka_conf_set_opaque(rkc, cycle);
    rd_kafka_conf_set_dr_msg_cb(rkc, ngx_http_klm_msg_cb);
    rd_kafka_conf_set_log_cb(rkc, ngx_http_klm_log_cb);

    if( !(mcf->rk = rd_kafka_new(RD_KAFKA_PRODUCER, rkc
                                , errstr, sizeof(errstr))) 
      )
    {
        ngx_log_error(NGX_LOG_ERR, cycle->pool->log, 0, "%s", errstr);
        return NGX_ERROR;
    }

    cln = ngx_pool_cleanup_add(cycle->pool, 0);
    if( cln == NULL )
        return NGX_ERROR;

    cln->handler = ngx_http_klm_kafka_destroy;
    cln->data    = mcf;

    for( list = mcf->topics;
         (topic = list->item) != NULL;
         list = list->next )
    {
        if( !(rktc = rd_kafka_topic_conf_new()) )
        {
            ngx_log_error(NGX_LOG_ERR, cycle->pool->log, 0
                                     , "Cannot create topic conf");
            return NGX_ERROR;
        }

        if( RD_KAFKA_CONF_OK != 
            rd_kafka_topic_conf_set(rktc, "request.required.acks", "0"
                            , errstr, sizeof(errstr)) )
        {
            ngx_log_error(NGX_LOG_ERR, cycle->pool->log, 0
                                    , "Cannot set topic configuration");
            return NGX_ERROR;
        }

        topic->rkt = rd_kafka_topic_new(mcf->rk, (char *)topic->name.data, rktc);
        if( topic->rkt == NULL )
        {
            ngx_log_error(NGX_LOG_ERR, cycle->pool->log, 0
                                     , "Creating topic %V", &topic->name);
            return NGX_ERROR;
        }

        topic->rk = mcf->rk;
    }

    return NGX_OK;
}

static int ngx_http_klm_err_rate_pass()
{
    time_t          ts;
    static int      cnt  = 10;
    static time_t   last = 0;

    ts = ngx_time() - last;
    if( ts > 5 ) {
        cnt  = 10;
        last = ngx_time();
        return 1;
    }

    if( cnt <= 0 )
        return 0;

    cnt--;

    return 1;
}

static void ngx_http_klm_fb_flush(ngx_open_file_t *open_file, ngx_log_t *errlog)
{
    ssize_t                  n, len;
    ngx_err_t                err;
    time_t                   now;
    ngx_http_klm_file_t     *file;

    file = open_file->data;

    len = file->pos - file->start;

    n = ngx_write_fd(open_file->fd, file->start, len);
    
    file->pos = file->start;

    if( n == len )
        return;

    now = ngx_time();

    err = ngx_errno;
    if( n == -1 )
    {
        if( err == NGX_ENOSPC )
            file->disk_full_time = now;

        if( now - file->error_log_time > 59 )
        {
            ngx_log_error( NGX_LOG_ALERT, errlog, err
                , ngx_write_fd_n " to \"%s\" log failed"
                , open_file->name.len, open_file->name.data);
            file->error_log_time = now;
        }

        return;
    }

    if( now - file->error_log_time > 59 )
    {
        ngx_log_error( NGX_LOG_ALERT, errlog, err
            , ngx_write_fd_n " to log write incomplete: %z of %uz"
            , n, len);
        file->error_log_time = now;
    }
}

static void ngx_http_klm_log_file(ngx_http_klm_file_t *file, ngx_str_t *msg, ngx_str_t *key)
{
    size_t  len;

    len = msg->len + key->len + 2;
    if( (size_t)(file->end - file->pos) < len )
    {
        ngx_http_klm_fb_flush(file->open_file, ngx_cycle->log);
        if( (size_t)(file->end - file->start) < len )
        {
            len = len + (NGX_HTTP_KLM_FILE_BUF_SIZE 
                         - len % NGX_HTTP_KLM_FILE_BUF_SIZE);
            if( len > NGX_HTTP_KLM_FILE_BUF_SIZE_MAX )
            {
                ngx_log_error( NGX_LOG_WARN, ngx_cycle->log, 0
                    , "buffer request of %z bytes is too big at kafka_log_module "
                    , len);
                return;
            }

            ngx_pfree(ngx_cycle->pool, file->start);

            file->start = ngx_palloc(ngx_cycle->pool, len);
            if( !file->start )
            {
                ngx_log_error( NGX_LOG_CRIT, ngx_cycle->log, 0
                     , "Cannot alocate memory of size %z for kafka file buffer"
                     , len);
                return;
            }
            file->end = file->start + len;
            file->pos = file->start;
        }
    }

    if( key->len )
    {
        file->pos = ngx_cpymem(file->pos
                                , key->data, key->len);
        *file->pos++ = '\t';
    }

    file->pos = ngx_cpymem(file->pos
                            , msg->data, msg->len);
    *file->pos++ = '\n';
}

static void ngx_http_klm_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    ngx_cycle_t             *cycle = opaque;
    ngx_http_klm_log_t      *log = rkmessage->_private;
    ngx_str_t                msg, key;

    if( !rkmessage->err )
        return;

    msg.data = rkmessage->payload;
    msg.len  = rkmessage->len;
    key.data = rkmessage->key;
    key.len  = rkmessage->key_len;

    if( log->file )
        ngx_http_klm_log_file(log->file, &msg, &key);

    if( ngx_http_klm_err_rate_pass() )
    {
	    ngx_log_error(NGX_LOG_WARN, cycle->pool->log, 0
            , "Message delivery failed. "
            , "Error (%d) '%s', msg: '%V', key:'%V', qlen: %d "
            , rkmessage->err, rd_kafka_err2str(rkmessage->err)
            , &msg, &key, rd_kafka_outq_len(rk));
    }
}


static void ngx_http_klm_log_cb(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
    if( ngx_http_klm_err_rate_pass() )
    {
        if( (unsigned int)level > 8 )
            level = 8;

        ngx_log_error( (unsigned int)level, ngx_cycle->log, 0
            , "(%s) %s: %s"
            , fac, rk ? rd_kafka_name(rk) : NULL, buf);
    }
}


static void ngx_http_klm_kafka_destroy(void *user)
{
    ngx_http_klm_main_conf_t      *mcf = user;
    ngx_http_klm_topic_t          *topic;
    ngx_http_klm_list_t           *list;
    
    rd_kafka_flush(mcf->rk, 3000);

    for( list = mcf->topics; (topic = list->item) != NULL; list = list->next )
    {
        rd_kafka_topic_destroy(topic->rkt);
    }

    rd_kafka_destroy(mcf->rk);
}


