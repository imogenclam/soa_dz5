FROM grafana/grafana:12.4.6

ENV GF_PATHS_PLUGINS=/opt/grafana-plugins \
    GF_PLUGINS_PREINSTALL_DISABLED=true \
    GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/dashboards/cinema.json

USER root
RUN mkdir -p "$GF_PATHS_PLUGINS" && chown 472:0 "$GF_PATHS_PLUGINS"
USER 472
RUN grafana cli --pluginsDir "$GF_PATHS_PLUGINS" plugins install grafana-clickhouse-datasource 4.21.2

COPY provisioning/ /etc/grafana/provisioning/
COPY dashboards/ /etc/grafana/dashboards/
