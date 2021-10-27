# 拆分基础镜像： docker/dockerfile


# 基础镜像，按照季度，月度更新。不影响应用镜像的构建。

FROM dxglaw/pythonstock:base-2021-10

WORKDIR /data

#add cron sesrvice.
#每分钟，每小时1分钟，每天1点1分，每月1号执行
# do not use /etc/cron.hourly as they can be called automatically which results in twice run of the job.
RUN mkdir -p /etc/jobs/cron.minutely && \
    mkdir -p /etc/jobs/cron.hourly && \
    mkdir -p /etc/jobs/cron.daily && \
    mkdir -p /etc/jobs/cron.monthly && \
    echo "SHELL=/bin/sh \n\
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin \n\
# min   hour    day     month   weekday command \n\
*/1     *       *       *       *       /bin/run-parts /etc/jobs/cron.minutely \n\
10       *       *       *       *       /bin/run-parts /etc/jobs/cron.hourly \n\
30       17       *       *       *       /bin/run-parts /etc/jobs/cron.daily \n\
30       17       1,10,20       *       *       /bin/run-parts /etc/jobs/cron.monthly \n" > /var/spool/cron/crontabs/root && \
    chmod 600 /var/spool/cron/crontabs/root


#增加服务端口就两个 6006 8500 9001
EXPOSE 8888 9999

#经常修改放到最后：
ADD jobs /data/stock/jobs
ADD libs /data/stock/libs
ADD web /data/stock/web
ADD supervisor /data/supervisor

# chmod below cannot work on files or dirs in a VOLUME.
# copy to another direction to change paermission.
ADD jobs/cron.minutely /etc/jobs/cron.minutely
ADD jobs/cron.hourly /etc/jobs/cron.hourly
ADD jobs/cron.daily /etc/jobs/cron.daily
ADD jobs/cron.monthly /etc/jobs/cron.monthly

RUN mkdir -p /data/logs && \
    ls /data/stock/ && \
    chmod 755 /data/stock/jobs/run_* &&  \
    chmod 755 /etc/jobs/cron.minutely/* && \
    chmod 755 /etc/jobs/cron.hourly/* && \
    chmod 755 /etc/jobs/cron.daily/* && \
    chmod 755 /etc/jobs/cron.monthly/*
# RUN mkdir -p /data/logs && ls /data/stock/ && chmod 755 /data/stock/jobs/run_* &&  \
#     chmod 755 /etc/cron.minutely/* && chmod 755 /etc/cron.hourly/* && \
#     chmod 755 /etc/cron.daily/* && chmod 755 /etc/cron.monthly/*

ENTRYPOINT ["supervisord","-n","-c","/data/supervisor/supervisord.conf"]