#!/bin/sh

# checks if locally staged changes are
# formatted properly. Ignores non-staged
# changes.
# Intended as git pre-commit hook

_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DIR=$( echo ${_DIR} | sed 's/\/.git\/hooks$//' )

#COLOR CODES:
#tput setaf 3 = yellow -> Info
#tput setaf 1 = red -> warning/not allowed commit
#tput setaf 2 = green -> all good!/allowed commit

echo ""
echo "$(tput setaf 3)Running pre-commit hook ... (you can omit this with --no-verify, but don't)$(tput sgr 0)"
git diff --quiet
hadNoNonStagedChanges=$?

if ! [[ ${hadNoNonStagedChanges} -eq 0 ]]
then
   echo "$(tput setaf 3)* Stashing non-staged changes$(tput sgr 0)"
   git stash --keep-index -u > /dev/null
fi

echo "$(tput setaf 3)* Formatting staged changes... $(tput sgr 0)"
(cd ${DIR}/core/; mvn org.antipathy:mvn-scalafmt_2.12:0.10_1.5.1:format)
(cd ${DIR}/tikv-client/; mvn com.coveo:fmt-maven-plugin:2.6.0:format)
(cd ${DIR}/spark-wrapper/spark-2.3/; mvn org.antipathy:mvn-scalafmt_2.11:0.10_1.5.1:format)
(cd ${DIR}/spark-wrapper/spark-2.4/; mvn org.antipathy:mvn-scalafmt_2.12:0.10_1.5.1:format)
(cd ${DIR}/spark-wrapper/spark-3.0/; mvn org.antipathy:mvn-scalafmt_2.12:0.10_1.5.1:format)
(cd ${DIR}/)
git diff --quiet
formatted=$?

echo "$(tput setaf 3)* Properly formatted?$(tput sgr 0)"

if [[ ${formatted} -eq 0 ]]
then
   echo "$(tput setaf 2)* Yes$(tput sgr 0)"
else
   echo "$(tput setaf 1)* No$(tput sgr 0)"
    echo "$(tput setaf 1)The following files need formatting (in stage or commited):$(tput sgr 0)"
    git diff --name-only
    echo ""
    echo "$(tput setaf 1)Files above will be formatted forcibly.$(tput sgr 0)"
    echo ""
fi

if ! [[ ${hadNoNonStagedChanges} -eq 0 ]]
then
   echo "$(tput setaf 3)* Scheduling stash pop of previously stashed non-staged changes for 1 second after commit.$(tput sgr 0)"
   if [[ ${formatted} -eq 0 ]]
   then
      sleep 1 && git stash pop --index > /dev/null & # sleep and & otherwise commit fails when this leads to a merge conflict
   else
      echo "$(tput setaf 3)* Undoing formatting$(tput sgr 0)"
      git stash --keep-index > /dev/null
      git stash drop > /dev/null
      git stash pop --index > /dev/null &
      echo "$(tput setaf 1)... done.$(tput sgr 0)"
      echo "$(tput setaf 1)CANCELLING commit due to NON-FORMATTED CODE.$(tput sgr 0)"
      echo "$(tput setaf 1)You probably need to stash your changes to avoid format conflicts.$(tput sgr 0)"
      echo ""
      exit 1
   fi
fi

if ! [[ ${formatted} -eq 0 ]]
then
   echo "$(tput setaf 2)Add format result$(tput sgr 0)"
   git add .
fi

echo "$(tput setaf 2)... done. Proceeding with commit.$(tput sgr 0)"
echo ""
echo 0
