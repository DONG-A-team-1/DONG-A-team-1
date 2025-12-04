# 프로젝트 레포지토리 사용 가이드 (GitHub Flow)

이 레포지토리는 **GitHub Flow**를 사용합니다.  
즉, **항상 main에서 새 브랜치를 따서 작업 → PR → 코드 리뷰 → main에 머지 → 배포**의 흐름으로 진행합니다.

---

## 0. 기본 원칙 (GitHub Flow)

- `main` 브랜치는 항상 **배포 가능한 상태**를 유지합니다.
- 모든 작업은 **반드시 새 브랜치**에서 진행합니다.
- 기능 단위로 **작고 자주** PR을 만듭니다.
- PR은 반드시 **코드 리뷰 후** `main`에 머지합니다.

---

## 1. 레포지토리 클론

# 원하는 디렉토리로 이동
cd D:\STUDY\project
# 레포지토리 클론
git clone https://github.com/ORG_NAME/REPO_NAME.git
# 폴더로 이동
cd REPO_NAME

----

## 2 main 최신 상태로 맞추기
작업 시작 전 항상 main을 최신으로 맞춰줍니다

git switch main
git pull origin main

----

## 3.작업 브랜치 생성
브랜치 이름은 작업 내용을 알 수 있도록 설정합니다
기능 개발: feature/기능이름

git switch -c feature/브랜치이름

----

## 4.코드 수정 후 변경 사항 확인

git status

----

## 5.변경 사항 추적 및 커밋하기

변경된 파일 모두 추가 (주의해서 사용)
git add .

또는 파일을 개별적으로 추가
git add path/to/file1 path/to/file2

커밋 (현재형, 짧고 명확하게)
git commit -m "Implement article list page"

----
## 6.작업 브랜치 푸쉬

처음한번은 -u를 통한 upstream 설정을 해줍니다
git push -u origin feature/브랜치 이름

이후로는 간단하게 
git push

----

## 7.Pull request 생성하기 

GitHub에서 레포지토리 페이지로 이동
방금 푸시한 브랜치에 대해 “Compare & pull request” 버튼 클릭
(또는 Pull requests → New pull request 진입)

아래와 같이 설정
base: main
compare: feature/브랜치-이름

PR 제목/내용 작성
무엇을 했는지
UI 변경이면 스크린샷 첨부
테스트 방법, 참고사항 등
리뷰어 지정 후 Create pull request

----

## 8. 코드 리뷰 → 수정 → 머지

리뷰 코멘트가 달리면:
로컬에서 수정 → git add → git commit → git push
동일 PR에 자동으로 반영됩니다.

리뷰 승인 후:
Merge pull request로 main에 머지
필요 시 “Squash and merge” 사용 (팀 규칙에 따름)
머지 후에는 main이 곧 배포 대상 브랜치가 됩니다.

----

## 9.머지 후 브랜치 정리
작업이 완료된 이후 사용한 브랜치를 닫습니다

main으로 이동하고 최신 상태 반영
git switch main
git pull origin main

로컬 브랜치 삭제
git branch -d feature/브랜치-이름

원격 브랜치 삭제 (선택)
git push origin --delete feature/브랜치-이름

----

## 10.이후 새로운 작업을 시작하며 반복
git switch main
git pull origin main
git switch -c feature/새-작업-브랜치


