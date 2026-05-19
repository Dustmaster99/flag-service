# =========================
# Stage 1 — Build
# =========================
FROM python:3.12-slim AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Dependências de build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

# Copia requirements
COPY requirements.txt .

# Cria ambiente virtual
RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

# Instala dependências
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt \
 && python -c "import setuptools; import pkg_resources; print('setuptools ok')"


# =========================
# Stage 2 — Runtime
# =========================
FROM python:3.12-slim AS runtime

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:$PATH"

# Dependência runtime PostgreSQL (libpq)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
 && rm -rf /var/lib/apt/lists/*

# Copia ambiente virtual pronto
COPY --from=builder /opt/venv /opt/venv

# Copia aplicação
COPY app.py .
COPY db ./db

# Expõe porta
EXPOSE ${PORT}

# Inicialização da aplicação
CMD ["sh", "-c", ": \"${PORT:?PORT environment variable is required}\" && python app.py --port ${PORT}"]