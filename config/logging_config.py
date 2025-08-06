import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Optional

from config.settings import settings

class ETLLogger:
    """ETL 프로세스용 로거"""
    
    def __init__(self, name: str = 'etl'):
        self.name = name
        self.logger = None
        self._setup_logger()
    
    def _setup_logger(self):
        """로거 설정"""
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG if settings.DEBUG else logging.INFO)
        
        # 이미 핸들러가 있으면 스킵
        if self.logger.handlers:
            return
        
        # 로그 디렉토리 생성
        log_dir = settings.LOG_DIR / datetime.now().strftime('%Y%m%d')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # 파일 핸들러 (일별 로테이션)
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=log_dir / f'{self.name}.log',
            when='midnight',
            interval=1,
            backupCount=30,
            encoding='utf-8'
        )
        
        # 콘솔 핸들러
        console_handler = logging.StreamHandler()
        
        # 포맷터
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - '
            '[%(filename)s:%(lineno)d] - %(message)s'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_logger(self) -> logging.Logger:
        """로거 인스턴스 반환"""
        return self.logger

# 기본 로거 인스턴스
etl_logger = ETLLogger().get_logger()