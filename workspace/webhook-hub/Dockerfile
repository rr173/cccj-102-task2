FROM python:3.11-slim

WORKDIR /app
COPY whub ./whub

# 纯标准库，无第三方依赖；默认启动投递中枢
ENV WHUB_HOST=0.0.0.0 \
    WHUB_PORT=8080 \
    WHUB_DB=/data/whub.db
EXPOSE 8080
VOLUME ["/data"]

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=2).status==200 else 1)"

ENTRYPOINT ["python3", "-m", "whub"]
CMD ["hub"]
