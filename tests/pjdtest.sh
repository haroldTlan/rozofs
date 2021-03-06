#!/bin/bash

#  Copyright (c) 2010 Fizians SAS. <http://www.fizians.com>
#  This file is part of Rozofs.
#  Rozofs is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published
#  by the Free Software Foundation, version 2.
#  Rozofs is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see
#  <http://www.gnu.org/licenses/>.

#
# pjdtest.sh 
#

. env.sh

#$1: mount point
pjdtest() {
    flog=${WORKING_DIR}/pjdtest_`date "+%Y%m%d_%Hh%Mm%Ss"`_`basename $1`.log
    cd $1
    # SLEEP TIME BEFORE START TEST
    sleep 2
    export PJD_TRACE=${LOCAL_PJDTESTS}/result
    prove -r ${LOCAL_PJDTESTS} 2>&1 | tee -a $flog
    EXIT_CODE=${PIPESTATUS[0]}
    cd ${WORKING_DIR}
    case "${EXIT_CODE}" in
      0);;
      *) echo "See execution traces under $PJD_TRACE";;
    esac  
    return ${EXIT_CODE}
}

usage() {
    echo "$0: <mount point>"
    exit 1  
}

[[ $# -lt 1 ]] && usage

pjdtest $1

exit $?
