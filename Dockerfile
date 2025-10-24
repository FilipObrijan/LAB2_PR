FROM python:3.12-slim
WORKDIR /app
COPY server_mt.py request_test.py ./
ENV PORT=8001
EXPOSE 8001
CMD ["python", "server_mt.py", "/serve"]
