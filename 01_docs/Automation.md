
# Automation

```
본 문서는 Streamlit 기반 데이터 파이프라인 자동화 환경의 구조와 
실행 방식을 정의하기 위해 작성되었습니다.

원본 데이터 업로드부터 데이터 적재, SQL 파이프라인 실행, 분석 결과 생성까지의 전체 프로세스를
일관된 방식으로 자동화하는 것을 목적으로 합니다.

이를 통해 사용자는 별도의 경로 설정 없이 데이터 업로드와 버튼 클릭만으로
전체 분석 파이프라인을 실행할 수 있습니다.
```


---

## Architecture

```
본 자동화 파이프라인은 아래와 같은 구조로 설계되었습니다.

Raw → Staging → Data Mart → Analytics → BI → Output

각 레이어는 데이터 처리 단계별 역할에 따라 분리되어 있으며,
SQL 기반으로 순차적으로 실행됩니다.

이를 통해 데이터 흐름을 구조화하고, 유지보수성과 확장성을 확보하는 것을 목표로 합니다.
```

- **Raw**
	- 원본 CSV 데이터를 적재하는 단계
	- 데이터 변형 없이 저장되는 초기 입력 영역

- **Staging**
	- 데이터 정제 및 전처리 수행
	- 데이터 품질 보정 및 형식 통일

- **Data Mart**
	- 분석을 위한 구조화된 데이터 구성
	- KPI 및 분석에 최적화된 테이블 설계

- **Analytics**
	- KPI 계산 및 분석 결과 생성
	- 비즈니스 로직 기반 데이터 집계

- **BI**
	- 대시보드 시각화를 위한 데이터 생성
	- BI 도구(Tableau, Streamlit) 연결용 데이터 제공


---

## Automation Flow

```
Streamlit UI를 통해 데이터 업로드부터 파이프라인 실행까지 전체 프로세스를 자동화합니다.

사용자 입력을 기반으로 Raw 데이터 적재부터 SQL 파이프라인 실행, 
결과 생성까지의 흐름이 순차적으로 수행됩니다.

이를 통해 사용자 개입을 최소화하고, 일관된 데이터 처리 과정을 보장하는 것을 목표로 합니다.

자동화 흐름은 다음과 같습니다.

- Data Upload
- Raw Data Save
- Data Load
- SQL Pipeline Execution
- Output Generation
```

- **1. Data Upload**
	- 사용자가 Streamlit UI에서 원본 CSV 파일 업로드

- **2. Raw Data Save**
	- 업로드된 파일을 프로젝트 내부 경로(`00_data/01_raw`)에 자동 저장
	- 경로가 존재하지 않을 경우 자동 생성

- **3. Data Load**
	- Raw 데이터를 데이터베이스에 적재

- **4. SQL Pipeline Execution**
	- Staging → Data Mart → Analytics → BI 순으로 SQL 스크립트 실행

- **5. Output Generation**
	- 분석 결과 및 KPI 데이터 생성
	- 결과 데이터를 `outputs/` 폴더에 저장


---

## Data Handling

```
모든 데이터는 프로젝트 내부 경로를 기준으로 관리됩니다.

경로 의존성을 제거하여 실행 환경에 관계없이 동일한 구조로 동작하도록 설계되었습니다.

데이터 저장 및 로딩 경로는 자동으로 생성 및 관리되어 사용자의 별도 설정이 필요하지 않습니다.

데이터 저장 및 로딩 경로는 다음과 같습니다.

- Raw Data
	- 경로: 00_data/01_raw/
	- 업로드된 CSV 파일이 자동으로 저장되는 위치
	- 경로가 존재하지 않을 경우 자동 생성

- Output Data
	- 경로: outputs/
	- 파이프라인 실행 결과가 저장되는 위치

- Path Management
	- 프로젝트 루트 기준 상대 경로 사용
	- 실행 환경에 관계없이 동일하게 동작
```


---

## Execution

```
Streamlit 애플리케이션을 실행한 후 데이터 업로드와 버튼 클릭만으로 
전체 파이프라인을 수행할 수 있습니다.

환경 설정 이후 별도의 수동 작업 없이 자동화된 흐름에 따라 데이터 처리와 분석이 진행됩니다.

사용자는 UI 기반 인터페이스를 통해 직관적으로 파이프라인을 제어할 수 있습니다.
```


### Environment Requirements

```
실행을 위해 필요한 환경은 다음과 같습니다.

- MySQL 환경 설치 및 서버 실행
- Python 환경 설치
- Raw 데이터 다운로드
- 실행 코드 입력
```

- **MySQL 환경 설치**
	- MySQL 환경은 공식 사이트를 통해 설치
	- 설치 이후 .env.example과 같이 환경 변수 설정 파일 생성(.env 파일)

- **Python 환경 설치**
	- Python 환경에서 requirements.txt를 설치 (requirements.txt는 깃허브를 통해 다운)
	- `pip install -r requirements.txt`

- **Raw 데이터 다운로드**
	- README의 데이터 Source 링크를 통해 다운

- **실행 코드**

```bash
streamlit run 05_app/app.py
```
