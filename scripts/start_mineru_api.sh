#!/usr/bin/env bash
set -euo pipefail

MINERU_VENV="${MINERU_VENV:-/home/zhangbeiqing/venv/pdf2markdown}"
MINERU_SOURCE="${MINERU_SOURCE:-/home/zhangbeiqing/programer/MinerU}"
MINERU_HOST="${MINERU_HOST:-127.0.0.1}"
MINERU_PORT="${MINERU_PORT:-8000}"
CUDA_HOME="${CUDA_HOME:-/home/zhangbeiqing/.local/cuda-13-root/usr/local/cuda-13.0}"

if [[ ! -x "${MINERU_VENV}/bin/mineru-api" ]]; then
    echo "MinerU API 不存在: ${MINERU_VENV}/bin/mineru-api" >&2
    exit 1
fi

if [[ ! -x "${CUDA_HOME}/bin/nvcc" ]]; then
    echo "CUDA 13 编译器不存在: ${CUDA_HOME}/bin/nvcc" >&2
    exit 1
fi

export CUDA_HOME
export FLASHINFER_NVCC="${FLASHINFER_NVCC:-${CUDA_HOME}/bin/nvcc}"
export PATH="${MINERU_VENV}/bin:${CUDA_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${CUDA_HOME}/lib64:${LD_LIBRARY_PATH:-}"
export MINERU_MODEL_SOURCE="${MINERU_MODEL_SOURCE:-modelscope}"
export MINERU_API_MAX_CONCURRENT_REQUESTS="${MINERU_API_MAX_CONCURRENT_REQUESTS:-1}"
# MinerU 默认一次处理 64 页，数百页财报在内存有限的 WSL 中容易触发 OOM。
# 16 页仍保留 Hybrid/GPU 加速，只降低长文档的峰值系统内存。
export MINERU_PROCESSING_WINDOW_SIZE="${MINERU_PROCESSING_WINDOW_SIZE:-32}"
# 本地客户端会在完成后立即取结果，无需让批量任务及临时输出驻留 24 小时。
export MINERU_API_TASK_RETENTION_SECONDS="${MINERU_API_TASK_RETENTION_SECONDS:-60}"
export MINERU_API_TASK_CLEANUP_INTERVAL_SECONDS="${MINERU_API_TASK_CLEANUP_INTERVAL_SECONDS:-30}"

cd "${MINERU_SOURCE}"
exec "${MINERU_VENV}/bin/mineru-api" \
    --host "${MINERU_HOST}" \
    --port "${MINERU_PORT}" \
    --enable-vlm-preload true
