use std::sync::{Arc, Mutex};

/// Human: Reuse fixed-size read buffers for hot GET paths to cut allocator churn.
/// Agent: Mutex-backed Vec pool; acquire returns pooled or fresh buffer; release truncates and returns.
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<Mutex<PoolInner>>,
    capacity: usize,
}

struct PoolInner {
    buffers: Vec<Vec<u8>>,
    max_buffers: usize,
}

impl BufferPool {
    pub fn new(buffer_capacity: usize, max_buffers: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PoolInner {
                buffers: Vec::new(),
                max_buffers: max_buffers.max(4),
            })),
            capacity: buffer_capacity.max(4096),
        }
    }

    pub fn buffer_capacity(&self) -> usize {
        self.capacity
    }

    pub fn acquire(&self) -> Vec<u8> {
        let mut guard = self.inner.lock().expect("buffer pool lock");
        guard
            .buffers
            .pop()
            .map(|mut b| {
                b.clear();
                if b.capacity() < self.capacity {
                    b.reserve(self.capacity.saturating_sub(b.capacity()));
                }
                b
            })
            .unwrap_or_else(|| Vec::with_capacity(self.capacity))
    }

    pub fn release(&self, mut buf: Vec<u8>) {
        buf.clear();
        let mut guard = self.inner.lock().expect("buffer pool lock");
        if guard.buffers.len() < guard.max_buffers {
            guard.buffers.push(buf);
        }
    }
}
