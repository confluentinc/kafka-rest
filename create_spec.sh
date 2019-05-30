#!/bin/bash

# create_spec.sh spec.in out.spec
CONFLUENT_PATCH = "cp"
set -e

echo "Creating the spec file"
if [ -n ${REVISION} ]; then
  RPM_RELEASE_ID=${REVISION}
else
  RPM_RELEASE_ID=1
fi
if [ -n "${RPM_RELEASE_POSTFIX}" ]; then
    if [[ "${RPM_RELEASE_POSTFIX}" == *$CONFLUENT_PATCH* ]]; then
      RPM_RELEASE_ID="${RPM_RELEASE_ID}.${RPM_RELEASE_POSTFIX}"
    else
      RPM_RELEASE_ID="0.${RPM_RELEASE_ID}.${RPM_RELEASE_POSTFIX}"
    fi
fi

cat $1 | sed "s@##RPMVERSION##@${RPM_VERSION}@g; s@##RPMRELEASE##@${RPM_RELEASE_ID}@g" > $2
