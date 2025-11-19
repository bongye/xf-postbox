# S&P Global Xpressfeed Downloader

S&P Global Xpressfeed 데이터를 자동으로 다운로드하는 Python 스크립트입니다. SFTP를 통해 최신 Full/Change 파일을 효율적으로 다운로드하며, 병렬 처리와 실시간 진행률 표시를 지원합니다.

## 주요 기능

- ✅ **자동 파일 필터링**: 최신 Full 파일과 그 이후의 Change 파일만 선택적으로 다운로드
- ✅ **병렬 다운로드**: 멀티스레드 기반 동시 다운로드로 속도 향상
- ✅ **실시간 진행률**: Rich 라이브러리 기반의 아름다운 Progress Bar
- ✅ **Dry-run 모드**: 실제 다운로드 전 파일 크기 확인 및 CSV 저장
- ✅ **안전한 종료**: Ctrl+C로 graceful shutdown 지원
- ✅ **설정 파일 기반**: YAML 설정으로 쉬운 패키지 관리

## 요구사항

- Python 3.7+
- 필요한 패키지들:
```bash
pip install -r requirements.txt
```

**requirements.txt:**
```
paramiko>=3.4.0
rich>=13.7.0
PyYAML>=6.0.1
```

## 설치

1. 저장소 클론 또는 파일 다운로드
```bash
git clone <repository-url>
cd xf-postbox
```

2. 필요한 패키지 설치
```bash
pip install -r requirements.txt
```

3. 설정 파일 생성
```bash
cp config.yaml.example config.yaml
```

## 설정

`config.yaml` 파일을 수정하여 연결 정보와 다운로드 옵션을 설정합니다:
```yaml
# SFTP 연결 정보
connection:
  host: your_ftp_host
  username: your_username
  password: your_password
  destination: /path/to/destination

# 다운로드할 패키지 목록
packages:
  products:
    - SNLBankBranchesData
    - SNLBankBranchesReferenceV2
    - SNLBankRegCurrentLatestUS
    - SNLCorporateData
    # 더 추가...
  
  xpressfeed:
    # Xpressfeed 패키지 (필요시 추가)
    # - aBANK01

# 다운로드 설정
download:
  # 동시 다운로드 스레드 수 (null이면 CPU 코어 수 - 1)
  thread_count: null
  
  # 파일 타입별 다운로드 여부
  file_types:
    full_files: true
    change_files: true
    flag_files: true
    config_files: true

# 디렉토리 설정
directories:
  - Products
  - Inbox
  - Outbox
  - Xpressfeed
```

### 패키지 필터링

- **특정 패키지만 다운로드**: `packages` 섹션에 원하는 패키지 리스트 작성
- **모든 패키지 다운로드**: 빈 리스트 `[]` 사용
```yaml
packages:
  products: []  # 모든 Products 패키지 다운로드
  xpressfeed: []  # 모든 Xpressfeed 패키지 다운로드
```

## 사용법

### 기본 다운로드
```bash
python xf-postbox.py
```

실행 후 다운로드할 파일 목록을 확인하고 진행 여부를 선택합니다.

### Dry-run 모드 (크기 확인만)
```bash
python xf-postbox.py --dry-run
```

실제 다운로드 없이 파일 크기만 확인하고 CSV로 저장합니다. 생성되는 CSV 파일에는 다음 정보가 포함됩니다:
- 디렉토리
- 패키지명
- 파일명
- 파일 크기 (bytes 및 읽기 쉬운 형식)

### 도움말
```bash
python xf-postbox.py --help
```

## 동작 방식

### 파일 필터링 로직

1. **Full 파일**: 가장 최신 타임스탬프를 가진 Full 파일들을 선택
2. **Change 파일**: Full 파일의 날짜(앞 8자리) 이상인 모든 Change 파일 선택

예시:
```
Full 파일: SNL_Full_20241117.zip (타임스탬프: 20241117)
→ 20241117 이상의 Change 파일들만 다운로드
  - SNL_Change_20241117.zip ✓
  - SNL_Change_20241118.zip ✓
  - SNL_Change_20241116.zip ✗
```

### 다운로드 프로세스

1. SFTP 서버 연결
2. 설정된 패키지 스캔
3. 필터링 규칙에 따라 다운로드 파일 선택
4. 병렬 다운로드 시작
5. 실시간 진행률 표시

## 진행률 표시

스크립트는 두 가지 진행률을 표시합니다:
```
전체 진행률 (5/10 files) ████████░░ 50% ⏱️ 00:02:30
  ↳ SNL_Full_20241117.zip 350MB 70MB/s 70%
  ↳ SNL_Change_20241118.zip 120MB 30MB/s 40%
```

- **전체 진행률**: 다운로드할 파일 개수 기준
- **개별 파일**: 각 파일의 바이트 단위 다운로드 진행률

## 종료 방법

### Graceful Shutdown (권장)
```
Ctrl + C (첫 번째)
```
진행 중인 다운로드를 완료한 후 종료합니다.

### 강제 종료
```
Ctrl + C (두 번째)
```
모든 다운로드를 즉시 중단하고 종료합니다.

## 출력 파일

### 일반 다운로드
```
destination/
├── Products/
│   ├── SNLBankBranchesData/
│   │   ├── SNL_Full_20241117.zip
│   │   └── SNL_Change_20241118.zip
│   └── SNLCorporateData/
│       └── ...
└── Xpressfeed/
    └── ...
```

### Dry-run 모드
```
download_estimate_20241118_143052.csv
```

CSV 파일 예시:
```csv
directory,package,filename,size_bytes,size_readable
Products,SNLBankBranchesData,SNL_Full_20241117.zip,524288000,500.00 MB
Products,SNLBankBranchesData,SNL_Change_20241118.zip,104857600,100.00 MB
,,,628145600,599.00 MB
```

## 문제 해결

### SFTP 연결 실패
- `config.yaml`의 연결 정보가 정확한지 확인
- 방화벽에서 포트 22(SFTP)가 허용되어 있는지 확인

### 패키지를 찾을 수 없음
- SFTP 서버에 해당 패키지가 존재하는지 확인
- `config.yaml`의 패키지명 철자 확인

### 다운로드 속도가 느림
- `config.yaml`의 `thread_count` 조정 (기본값: CPU 코어 수 - 1)
- 네트워크 대역폭 확인

### Ctrl+C가 즉시 반응하지 않음
- 첫 번째 Ctrl+C: 현재 파일 완료 후 종료
- 두 번째 Ctrl+C: 즉시 강제 종료

## 주의사항

⚠️ **비밀번호 보안**: `config.yaml` 파일에는 민감한 정보가 포함되므로 git에 커밋하지 마세요!
```bash
# .gitignore에 추가
config.yaml
download_estimate_*.csv
```

⚠️ **디스크 공간**: Full 파일은 매우 클 수 있으므로 충분한 디스크 공간을 확보하세요.

⚠️ **네트워크 안정성**: 대용량 파일 다운로드 시 안정적인 네트워크 연결이 필요합니다.

## 작성자

Won-hyung Park (S&P Global)
