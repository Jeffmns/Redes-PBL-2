# Estágio 1: Build
FROM golang:1.24-alpine AS builder

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia os arquivos de dependências primeiro (otimiza o cache das camadas)
COPY go.mod go.sum ./
RUN go mod download

# Copia o restante do código fonte
COPY . .

# Compila o binário. 
# CGO_ENABLED=0 garante que o binário seja estático e funcione em imagens leves.
RUN CGO_ENABLED=0 GOOS=linux go build -o main .

# ---
# Estágio 2: Final (Imagem de execução)
FROM alpine:latest

# Instala certificados CA (necessário para chamadas HTTPS externas)
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copia apenas o binário compilado do estágio anterior
COPY --from=builder /app/main .

# Porta que a aplicação costuma rodar
EXPOSE 8080

# Comando para iniciar a aplicação
CMD ["./main"]