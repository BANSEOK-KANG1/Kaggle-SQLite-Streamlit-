
# Olist E‑Commerce · Streamlit + Kaggle + SQLite + GitHub Actions (Starter)

이 저장소는 **Kaggle Olist 브라질 이커머스 데이터셋**(`olistbr/brazilian-ecommerce`)을
GitHub Actions로 **주기적 동기화(ETL)** 하고, SQLite 데이터베이스로 적재하여
**Streamlit 웹앱**에서 상호작용(필터, 지표, 차트)을 제공하는 스타터 프로젝트입니다.

## 빠른 시작 (로컬)

1) Python 3.10+ 권장.  
2) `pip install -r requirements.txt`  
3) `./.streamlit/secrets.toml` 파일 생성 후 아래 형식으로 Kaggle 토큰 입력:
```toml
[kaggle]
username = "YOUR_KAGGLE_USERNAME"
key = "YOUR_KAGGLE_KEY"
```
4) 초기 동기화(ETL):
```
python scripts/etl.py --download --load
```
5) 앱 실행:
```
streamlit run app.py
```

## GitHub Actions(배치 ETL)
- `.github/workflows/etl.yml`는 매일 새벽(UTC) ETL을 실행하고, 변경된 `data/olist.sqlite`를 커밋/푸시합니다.
- 리포 권한이 필요합니다(`permissions: contents: write`).  
- 저장소의 **Settings → Secrets and variables → Actions** 에 다음을 추가하세요:
  - `KAGGLE_USERNAME`
  - `KAGGLE_KEY`

## 주요 폴더
- `scripts/etl.py` : Kaggle에서 CSV 다운로드 → 정제 → SQLite 적재
- `db/models.py` : 간단한 SQLAlchemy 모델/헬퍼
- `app.py` : Streamlit 메인 앱
- `pages/` : 추가 대시보드/리포트 페이지 예시
- `data/` : SQLite 파일(`olist.sqlite`)과 원본 CSV 보관

## 참고 문서
- Kaggle API: https://www.kaggle.com/docs/api
- Streamlit secrets: https://docs.streamlit.io/develop/api-reference/connections/st.secrets
- GitHub Actions workflow: https://docs.github.com/actions/using-workflows/workflow-syntax-for-github-actions
- SQLAlchemy 2.0 튜토리얼: https://docs.sqlalchemy.org/orm/quickstart.html

