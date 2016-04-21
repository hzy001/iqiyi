# Mesos-Junkman

Mesos-Junkman 是 Mesos 集群的数据采集者，会采集所有我们感兴趣的数据并且通过
cloud-agent 发送到星盘，背后存储采用 openTSDB，然后再通过
[Grafana](http://gitlab.qiyi.domain/mesos/grafana) 来呈现。
