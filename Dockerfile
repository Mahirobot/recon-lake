FROM python:3.10-slim-bookworm

# openjdk-17-jdk-headless is required for PySpark's JVM. procps is required
# by PySpark's process management (Spark shells out to `ps` in some paths).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && readlink -f /usr/bin/java | sed "s:/bin/java::" > /etc/java_home_path

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# JAVA_HOME is resolved at build time (see above) rather than hardcoded,
# since the exact Debian OpenJDK path can shift between point releases.
# `ENV JAVA_HOME=...` cannot embed that build-time shell computation
# directly, so a thin entrypoint sources it before exec'ing whatever
# command is passed to `docker run` -- this image has no single fixed
# mode; it runs the pipeline, pytest, or streamlit depending on the CMD.
RUN printf '#!/bin/sh\nexport JAVA_HOME=$(cat /etc/java_home_path)\nexport PATH="$JAVA_HOME/bin:$PATH"\nexec "$@"\n' > /entrypoint.sh \
    && chmod +x /entrypoint.sh

EXPOSE 8501

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "--version"]
