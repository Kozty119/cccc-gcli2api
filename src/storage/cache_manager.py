"""
统一内存缓存管理器
为所有存储后端提供一致的内存缓存机制，确保读写一致性和高性能。
"""
import asyncio
import time
from typing import Dict, Any, Optional
from collections import deque
from abc import ABC, abstractmethod

from log import log


class CacheBackend(ABC):
    """缓存后端接口，定义底层存储的读写操作"""
    
    @abstractmethod
    async def load_data(self) -> Dict[str, Any]:
        """从底层存储加载数据"""
        pass
    
    @abstractmethod
    async def write_data(self, data: Dict[str, Any]) -> bool:
        """将数据写入底层存储"""
        pass


class UnifiedCacheManager:
    """统一缓存管理器"""
    
    def __init__(
        self,
        cache_backend: CacheBackend,
        cache_ttl: float = 300.0,
        write_delay: float = 1.0,
        name: str = "cache"
    ):
        """
        初始化缓存管理器
        
        Args:
            cache_backend: 缓存后端实现
            cache_ttl: 缓存TTL（秒）
            write_delay: 写入延迟（秒）
            name: 缓存名称（用于日志）
        """
        self._backend = cache_backend
        self._cache_ttl = cache_ttl
        self._write_delay = write_delay
        self._name = name
        
        # 缓存数据
        self._cache: Dict[str, Any] = {}
        self._cache_dirty = False
        self._last_cache_time = 0
        
        # 并发控制
        self._cache_lock = asyncio.Lock()
        
        # 异步写回任务
        self._write_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        
        # 性能监控
        self._operation_count = 0
        self._operation_times = deque(maxlen=1000)
    
    async def start(self):
        """启动缓存管理器"""
        if self._write_task and not self._write_task.done():
            return
        
        self._shutdown_event.clear()
        self._write_task = asyncio.create_task(self._write_loop())
        log.debug(f"{self._name} cache manager started")
    
    async def stop(self):
        """停止缓存管理器并刷新数据"""
        self._shutdown_event.set()
        
        if self._write_task and not self._write_task.done():
            try:
                await asyncio.wait_for(self._write_task, timeout=5.0)
            except asyncio.TimeoutError:
                self._write_task.cancel()
                log.warning(f"{self._name} cache writer forcibly cancelled")
        
        # 刷新缓存
        await self._flush_cache()
        log.debug(f"{self._name} cache manager stopped")
    
    async def get(self, key: str, default: Any = None) -> Any:
        """获取缓存项"""
        async with self._cache_lock:
            start_time = time.time()
            
            try:
                # 确保缓存已加载
                await self._ensure_cache_loaded()
                
                # 性能监控
                self._operation_count += 1
                operation_time = time.time() - start_time
                self._operation_times.append(operation_time)
                
                result = self._cache.get(key, default)
                log.debug(f"{self._name} cache get: {key} in {operation_time:.3f}s")
                return result
                
            except Exception as e:
                operation_time = time.time() - start_time
                log.error(f"Error getting {self._name} cache key {key} in {operation_time:.3f}s: {e}")
                return default
    
    async def set(self, key: str, value: Any) -> bool:
        """设置缓存项"""
        async with self._cache_lock:
            start_time = time.time()
            
            try:
                # 确保缓存已加载
                await self._ensure_cache_loaded()
                
                # 更新缓存
                self._cache[key] = value
                self._cache_dirty = True
                
                # 性能监控
                self._operation_count += 1
                operation_time = time.time() - start_time
                self._operation_times.append(operation_time)
                
                log.debug(f"{self._name} cache set: {key} in {operation_time:.3f}s")
                return True
                
            except Exception as e:
                operation_time = time.time() - start_time
                log.error(f"Error setting {self._name} cache key {key} in {operation_time:.3f}s: {e}")
                return False
    
    async def delete(self, key: str) -> bool:
        """删除缓存项"""
        async with self._cache_lock:
            start_time = time.time()
            
            try:
                # 确保缓存已加载
                await self._ensure_cache_loaded()
                
                if key in self._cache:
                    del self._cache[key]
                    self._cache_dirty = True
                    
                    # 性能监控
                    self._operation_count += 1
                    operation_time = time.time() - start_time
                    self._operation_times.append(operation_time)
                    
                    log.debug(f"{self._name} cache delete: {key} in {operation_time:.3f}s")
                    return True
                else:
                    log.warning(f"{self._name} cache key not found for deletion: {key}")
                    return False
                    
            except Exception as e:
                operation_time = time.time() - start_time
                log.error(f"Error deleting {self._name} cache key {key} in {operation_time:.3f}s: {e}")
                return False
    
    async def get_all(self) -> Dict[str, Any]:
        """获取所有缓存数据"""
        async with self._cache_lock:
            start_time = time.time()
            
            try:
                # 确保缓存已加载
                await self._ensure_cache_loaded()
                
                # 性能监控
                self._operation_count += 1
                operation_time = time.time() - start_time
                self._operation_times.append(operation_time)
                
                log.debug(f"{self._name} cache get_all ({len(self._cache)}) in {operation_time:.3f}s")
                return self._cache.copy()
                
            except Exception as e:
                operation_time = time.time() - start_time
                log.error(f"Error getting all {self._name} cache in {operation_time:.3f}s: {e}")
                return {}
    
    async def update_multi(self, updates: Dict[str, Any]) -> bool:
        """批量更新缓存项"""
        async with self._cache_lock:
            start_time = time.time()
            
            try:
                # 确保缓存已加载
                await self._ensure_cache_loaded()
                
                # 批量更新
                self._cache.update(updates)
                self._cache_dirty = True
                
                # 性能监控
                self._operation_count += 1
                operation_time = time.time() - start_time
                self._operation_times.append(operation_time)
                
                log.debug(f"{self._name} cache update_multi ({len(updates)}) in {operation_time:.3f}s")
                return True
                
            except Exception as e:
                operation_time = time.time() - start_time
                log.error(f"Error updating {self._name} cache multi in {operation_time:.3f}s: {e}")
                return False
    
    async def _ensure_cache_loaded(self):
        """确保缓存已从底层存储加载"""
        current_time = time.time()
        
        # 检查缓存是否需要加载（首次加载或过期）
        # 如果缓存脏了（有未写入的数据），不要重新加载以避免数据丢失
        if (self._last_cache_time == 0 or 
            (current_time - self._last_cache_time > self._cache_ttl and not self._cache_dirty)):
            
            await self._load_cache()
            self._last_cache_time = current_time
    
    async def _load_cache(self):
        """从底层存储加载缓存"""
        try:
            start_time = time.time()
            
            # 从后端加载数据
            data = await self._backend.load_data()
            
            if data:
                # 修复：合并而不是覆盖，以保留尚未写入的内存中更改
                # 以文件中的数据为基础，然后用当前内存中的缓存覆盖它
                # 这样可以保留脏数据，同时更新未更改的条目
                current_mem_cache = self._cache.copy()
                self._cache = data
                self._cache.update(current_mem_cache)
                log.debug(f"{self._name} cache merged ({len(self._cache)}) from backend")
            else:
                # 如果后端没有数据，则保持现有缓存不变（可能包含脏数据）
                log.debug(f"{self._name} cache initialized empty or backend is empty, keeping in-memory cache.")

            operation_time = time.time() - start_time
            log.debug(f"{self._name} cache loaded in {operation_time:.3f}s")

        except Exception as e:
            log.error(f"Error loading {self._name} cache from backend: {e}")
            # 出错时不应清空缓存，应保留内存中的数据
    
    async def _write_loop(self):
        """异步写回循环（优化，不在锁内执行I/O）"""
        while not self._shutdown_event.is_set():
            try:
                # 等待写入延迟或关闭信号
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=self._write_delay)
                    if self._shutdown_event.is_set():
                        break
                except asyncio.TimeoutError:
                    pass  # 正常超时，继续执行

                # 1. 在锁内快速复制数据，然后释放锁
                data_to_write = None
                async with self._cache_lock:
                    if self._cache_dirty:
                        data_to_write = self._cache.copy()

                # 2. 如果有数据，就在锁外执行I/O密集型操作
                if data_to_write:
                    await self._write_cache(data_to_write)

            except Exception as e:
                log.error(f"Error in {self._name} cache writer loop: {e}")
                await asyncio.sleep(5) # 出错时等待更长时间

    async def _write_cache(self, data_to_write: Dict[str, Any]):
        """将缓存写回底层存储（不在锁内执行）"""
        try:
            start_time = time.time()

            success = await self._backend.write_data(data_to_write)

            if success:
                operation_time = time.time() - start_time
                log.debug(f"{self._name} cache written to backend in {operation_time:.3f}s ({len(data_to_write)} items)")
                # 3. 写成功后，重新获取锁以更新状态
                async with self._cache_lock:
                    # 检查在写入期间缓存是否被再次修改
                    if self._cache == data_to_write:
                        self._cache_dirty = False
                    else:
                        log.debug("Cache modified during write, will write again in next cycle.")
            else:
                log.error(f"Failed to write {self._name} cache to backend. Will retry on next cycle.")

        except Exception as e:
            log.error(f"Error writing {self._name} cache to backend: {e}")
    
    async def _flush_cache(self):
        """立即刷新缓存到底层存储"""
        async with self._cache_lock:
            if self._cache_dirty:
                await self._write_cache()
                log.debug(f"{self._name} cache flushed to backend")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        avg_time = sum(self._operation_times) / len(self._operation_times) if self._operation_times else 0
        
        return {
            "cache_name": self._name,
            "cache_size": len(self._cache),
            "cache_dirty": self._cache_dirty,
            "operation_count": self._operation_count,
            "avg_operation_time": avg_time,
            "last_cache_time": self._last_cache_time,
        }