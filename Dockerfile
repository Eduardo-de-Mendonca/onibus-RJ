# Usa uma imagem leve do Python
FROM python:3.11-slim

# Define a pasta de trabalho
WORKDIR /app

# Copia os requisitos e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o resto do código
COPY . .

# Expõe a porta
EXPOSE 5000

# Comando para rodar o servidor
CMD ["python", "app.py"]
