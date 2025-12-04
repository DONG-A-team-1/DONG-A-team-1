# 📘 프로젝트 레포지토리 사용 가이드 (GitHub Flow)

이 레포지토리는 **GitHub Flow**를 사용합니다.  
즉, **항상 main에서 새 브랜치를 생성 → 작업 → PR → 코드 리뷰 → main 머지 → 배포**의 흐름으로 운영됩니다.

---

## 🔰 0. 기본 원칙 (GitHub Flow)

- `main` 브랜치는 항상 **배포 가능한 상태**로 유지합니다.
- 모든 작업은 **반드시 새 브랜치에서 진행**합니다.
- 기능 단위로 **작고 빠르게** PR을 생성합니다.
- PR은 반드시 **코드 리뷰 후** `main`에 머지합니다.

---

## 🚀 1. 레포지토리 클론


# 원하는 디렉토리로 이동
cd D:\STUDY\project

# 레포지토리 클론
git clone https://github.com/ORG_NAME/REPO_NAME.git

# 폴더로 이동
cd REPO_NAME
🔄 2. main 최신 상태로 맞추기
작업 시작 전, 항상 최신 상태로 동기화합니다.

bash
코드 복사
git switch main
git pull origin main
🌱 3. 작업 브랜치 생성
브랜치 이름은 작업 내용을 명확히 나타내도록 작성합니다.
예) feature/login-ui, fix/article-parser-bug

bash
코드 복사
git switch -c feature/브랜치-이름
📝 4. 코드 수정 후 변경 사항 확인
bash
코드 복사
git status
💾 5. 변경 사항 스테이징 및 커밋
bash
코드 복사
# 변경된 모든 파일 추가 (주의해서 사용)
git add .

# 또는 파일 개별 추가
git add path/to/file1 path/to/file2

# 커밋 (현재형, 짧고 명확하게 작성)
git commit -m "Implement article list page"
📤 6. 작업 브랜치 푸시
bash
코드 복사
# 최초 한 번 upstream 설정과 함께 푸시
git push -u origin feature/브랜치-이름

# 이후 동일 브랜치에서는 단순히:
git push
🔀 7. Pull Request 생성
GitHub 레포지토리 페이지로 이동

방금 푸시한 브랜치에 대해 Compare & pull request 클릭
(또는 Pull requests → New pull request)

아래 설정 확인

base: main

compare: feature/브랜치-이름

PR 제목/내용 작성

작업 내용 요약

UI 변경 시 스크린샷 첨부

테스트 방법/참고 사항

리뷰어 지정

Create pull request 클릭

👀 8. 코드 리뷰 → 수정 → 머지
🔎 리뷰 반영
bash
코드 복사
git add .
git commit -m "Fix: 리뷰 반영"
git push
PR에 자동 반영됩니다.

✅ 머지
리뷰 승인 후 Merge pull request 수행

필요 시 팀 규칙에 따라 Squash and merge 사용

머지된 main은 즉시 배포 가능한 상태가 됩니다.

🧹 9. 머지 후 브랜치 정리
bash
코드 복사
# main으로 이동 후 최신 상태 반영
git switch main
git pull origin main

# 로컬 브랜치 삭제
git branch -d feature/브랜치-이름

# 원격 브랜치 삭제 (선택)
git push origin --delete feature/브랜치-이름
🔁 10. 새로운 작업 시작 시 반복
bash
코드 복사
git switch main
git pull origin main
git switch -c feature/새-작업-브랜치
