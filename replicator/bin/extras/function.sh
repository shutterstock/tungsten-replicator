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
# This file contains few utility functions to query Replicator Node Manager state, change
# state or modify property file.
#
# Authors: Teemu Ollakka <teemu.ollakka@continuent.com>
#


#
# Set parameter value in properties file. 
#
# Arguments:
# $1 - Parameter name
# $2 - New value
#
function set_param
{
	local bak="${REPLICATOR_CONF}.bak.`date --rfc-3339=date`"
	cp ${REPLICATOR_CONF} "$bak"
	cat "$bak" | sed -e "s/\(${1}=\).*/\1${2}/" > ${REPLICATOR_CONF}
}

#
# Return the state of Replicator Node Manager 
#
# Returns: OFFLINE | SYNCHRONIZING | SLAVE | MASTER or empty string on failure.
#
function get_state
{
        ${CTRL} | grep 'State:' | sed -e "s/State\:\ \([[:alpha:]]*\).*/\1/"
}

#
# Wait until Replicator Node Manager reaches specified state.
#
# Arguments:
# $1 - State that should be reached until function returns.
#
function wait_state
{
	while test `get_state` != $1
	do
		sleep 1
	done
}

#
# Configure Replicator Node Manager, configuration is read from the file specified by 
# REPLICATOR_CONF environment variable.
#
function configure
{
	${CTRL} configure
}

#
# Reconfigure Replicator Node Manager, configuration is read from the file specified by
# REPLICATOR_CONF environment variable.
#
function reconfigure
{
	${CTRL} reconfigure
}

#
# Set Replicator Node Manager to SLAVE state. Function returns after state is reached.
#
function set_slave
{
	${CTRL} goOnline
	wait_state SLAVE
}

#
# Set Replicator Node Manager to MASTER state. This function calls first 'flush-logs', 
# then reads the name of the last binlog file and sets Extractor binlog file configuration
# parameters, calls reconfigure and gives goMaster command to ReplicatorManager. Function returns 
# after MASTER state is reached.
#
function set_master
{
	${CTRL} goMaster
	wait_state MASTER
}

