FROM elasticsearch:7.10.1
COPY target/releases/analytics_plugin-1.1.ES.7.10.1.zip /var/plugin.zip
RUN echo 'y' | elasticsearch-plugin install  file:///var/plugin.zip