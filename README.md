## git 사용 방법 안내

- 저희조는 git workflow 중 잦은 업데이트를 하는 소규모의 팀에 적합한 github flow를 사용할 예정입니다
- Git branch란?
  ----
  말 그대로 가지치듯 하나의 repository에 대한 여러개의 버젼이 공존할 수 있게 하는 방법입니다
  하나의 메인 브랜치, 저희의 경우 main 브랜치를 중심으로 작업할 예정입니다
  
- Github flow란?
  ----
  <img width="1280" height="406" alt="image" src="https://github.com/user-attachments/assets/1cee2f3b-0177-43f4-b9ed-17cf39966111" />
  이런 형태의 workflow 입니다.

  - main 브랜치는 가급적 직접 변경하지 않고 작업은 모두 개별의 branch에서 진행하고 작업이 완료되면 다시 main으로 변경 사항을 적용시키는 구조입니다
    
  - main 브랜치는 언제나 정상적으로 작동하는 상태를 유지합니다.
    
  - 모든 main으로의 merge는 팀원들과의 공유와 점검 이후에 진행합니다

- 작업 방법
  ----
  
  - 우선 원하는 비어있는 폴더에서
    
  ```bash
  # 레포지토리 클론
  git clone https://github.com/ORG_NAME/REPO_NAME.git

  # main 브랜치와의 동기화를 확인해줍니다
  git switch main
  git pull origin main

  # 작업을 진행할 브랜치를 생성 및 전환합니다
  git switch -c feature/기능명

  #작업 시작 전 main 브랜치에 있지 않은지 한번 확인하세요
  #참고로 origin이 붙는 명령어는 github에 올라와있는 원격 저장소를 직접 다룬다는 뜻입니다
  ```

  ----
  
  - 원하는 작업을 진행하며 단위별로 commit 합니다
  - commit은 일종의 스크린샷과 비슷한 개념입니다. 현재 작업 진도를 저장하고 추후 되돌리거나 확인할 수 있게 합니다
  - 모든 commit은 정확한 수정 사항과 그 내용에 대한 메시지를 남깁니다
  ```bash
  # 현재 브랜치 상태 확인하기
  git status
  
  # git이 추적할 파일 추가하기
  git add file_name.py

  # 작업 내용 저장
  git commit -m "작업 내용 상세하게"
  ```
- 작업 완료 시
  - 모든 작업은 직접 main으로 푸쉬가 아닌 브랜치에서 PR(pull request)를 생성하여 적용시킵니다
  - 방법은 아래와 같습니다
  ----
  ```bash
  git push -u origin feature/기능명

  # 환경에 따라 터미널에 직접 github 링크가 뜨기도, github에 들어가야 하기도 합니다
  # compare / pull request 화면에서 작업 내용을 상세하게 설명하고 팀원들에게 확인 받고 Merge pull request를 통해 반영
  # 만약 피드백을 받는다면 수정 후 다시 PR을 생성합니다
  ```
- 작업 종료
  
  - 작업이 종료되었다면 생성 브랜치를 삭제합니다
  ----
  ```bash
  # main으로 이동 후 깃허브의 상태 반영
  git switch main
  git pull origin main
  
  # 로컬 브랜치 삭제
  git branch -d feature/브랜치-이름
  
  # 원격 브랜치 삭제 (선택)
  git push origin --delete feature/브랜치-이름
  ```
  
  
