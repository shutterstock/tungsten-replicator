#!/bin/bash
#
# Tungsten: An Application Server for uni/cluster.
# Copyright (C) 2007-2008 Continuent Inc.
# Contact: tungsten@continuent.org
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of version 2 of the GNU General Public License as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
#
# Initial developer(s): Robert Hodges and Csaba Simon.
# Contributor(s):
#
#
# Description:
#
# This file contains routine to set Replicator Node Manager in the SLAVE state.
#
# Authors: Teemu Ollakka <teemu.ollakka@continuent.com>
#


set -e

if test -z $1
then
	echo "Usage: start_slave.sh <master_hostname>"
	exit 1
fi

REPLICATOR_BIN_DIR="$(dirname $0)"

. ${REPLICATOR_BIN_DIR}/env.sh

CTRL=${REPLICATOR_BIN_DIR}/replicatorctrl.sh

. ${REPLICATOR_BIN_DIR}/function.sh

state=`get_state`

if test -z $state
then
	echo "Failed to get state, is replicator.sh running?"
	exit 1
fi

set_param "replicator.thl.remote_uri" "thl:\/\/${1}\/"


case $state in
	OFFLINE)
		configure
		set_slave
		;;
	SYNCHRONIZING)
		reconfigure
		wait_state SLAVE
		;;
	SLAVE)
		reconfigure
		;;
	MASTER)
		echo "Can't start slave from MASTER state, please shutdown first"
		exit 1
		;;
esac
