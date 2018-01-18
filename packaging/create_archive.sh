#!/bin/bash
#
# Creates an archive suitable for distribution (standard layout for binaries,
# libraries, etc.).

set -e

if [ -z ${PACKAGE_TITLE} -o -z ${VERSION} -o -z ${DESTDIR} ]; then
    echo "PACKAGE_TITLE, VERSION, and DESTDIR environment variables must be set."
    exit 1
fi

BINPATH=${PREFIX}/bin
LIBPATH=${PREFIX}/share/${PACKAGE_TITLE}
DOCPATH=${PREFIX}/share/doc/${PACKAGE_TITLE}

INSTALL="install -D -m 644"
INSTALL_X="install -D -m 755"

rm -rf ${DESTDIR}${PREFIX}
mkdir -p ${DESTDIR}${PREFIX}
mkdir -p ${DESTDIR}${BINPATH}
mkdir -p ${DESTDIR}${LIBPATH}
mkdir -p ${DESTDIR}${SYSCONFDIR}

for svc in packaging/*.service ; do
    ${INSTALL} -o root -g root -D $svc ${DESTDIR}/lib/systemd/system/$(basename $svc)
done

PREPACKAGED="target/kafka-rest-${VERSION}-package"
pushd ${PREPACKAGED}
find bin/ -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL_X} -o root -g root XXX ${DESTDIR}${PREFIX}/XXX
find share/ -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL} -o root -g root XXX ${DESTDIR}${PREFIX}/XXX
pushd etc/kafka-rest/
find . -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL} -o root -g root XXX ${DESTDIR}${SYSCONFDIR}/XXX
popd
popd
