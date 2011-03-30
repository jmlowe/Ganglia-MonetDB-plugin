#/*******************************************************************************
#* Portions Copyright (C) 2008 Novell, Inc. All rights reserved.
#*
#* Redistribution and use in source and binary forms, with or without
#* modification, are permitted provided that the following conditions are met:
#*
#*  - Redistributions of source code must retain the above copyright notice,
#*    this list of conditions and the following disclaimer.
#*
#*  - Redistributions in binary form must reproduce the above copyright notice,
#*    this list of conditions and the following disclaimer in the documentation
#*    and/or other materials provided with the distribution.
#*
#*  - Neither the name of Novell, Inc. nor the names of its
#*    contributors may be used to endorse or promote products derived from this
#*    software without specific prior written permission.
#*
#* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS ``AS IS''
#* AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
#* ARE DISCLAIMED. IN NO EVENT SHALL Novell, Inc. OR THE CONTRIBUTORS
#* BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
#* CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
#* SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
#* INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
#* CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
#* ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
#* POSSIBILITY OF SUCH DAMAGE.
#*
#* Authors: Matt Ryan (mrayn novell.com)
#*                  Brad Nicholes (bnicholes novell.com)
#******************************************************************************/

import os
import rrdtool
import monetdb.sql
import logging
from time import time
from datetime import datetime
from Gmetad.gmetad_plugin import GmetadPlugin
from Gmetad.gmetad_config import getConfig, GmetadConfig

def get_plugin():
    ''' All plugins are required to implement this method.  It is used as the factory
        function that instanciates a new plugin instance. '''
    # The plugin configuration ID that is passed in must match the section name 
    #  in the configuration file.
    return MonetDBPlugin('rrd')

class MonetDBPlugin(GmetadPlugin):
    ''' This class implements the RRD plugin that stores metric data to RRD files.'''

    RRAS = 'RRAs'
    RRD_ROOTDIR = 'rrd_rootdir'

    # Default RRAs
    _cfgDefaults = {
            RRAS : [
                    'RRA:AVERAGE:0.5:1:244',
                    'RRA:AVERAGE:0.5:24:244',
                    'RRA:AVERAGE:0.5:168:244',
                    'RRA:AVERAGE:0.5:672:244',
                    'RRA:AVERAGE:0.5:5760:374'
            ],
            RRD_ROOTDIR : '@varstatedir@/ganglia/rrds',
    }

    def __init__(self, cfgid):
        self.rrdpath = None
        self.cfg = None
        self.kwHandlers = None
        self._resetConfig()
        
        # The call to the parent class __init__ must be last
        GmetadPlugin.__init__(self, cfgid)

    def _resetConfig(self):
        self.rrdpath = None
        self.cfg = MonetDBPlugin._cfgDefaults
        
        self.kwHandlers = {
            MonetDBPlugin.RRAS : self._parseRRAs,
            MonetDBPlugin.RRD_ROOTDIR : self._parseRrdRootdir
        }
    
    def _parseConfig(self, cfgdata):
        '''This method overrides the plugin base class method.  It is used to
            parse the plugin specific configuration directives.'''
        for kw,args in cfgdata:
            if self.kwHandlers.has_key(kw):
                self.kwHandlers[kw](args)

    def _parseRrdRootdir(self, arg):
        ''' Parse the RRD root directory directive. '''
        v = arg.strip().strip('"')
        if os.path.isdir(v):
            self.cfg[MonetDBPlugin.RRD_ROOTDIR] = v

    def _parseRRAs(self, args):
        ''' Parse the RRAs directive. '''
        self.cfg[MonetDBPlugin.RRAS] = []
        for rraspec in args.split():
            self.cfg[MonetDBPlugin.RRAS].append(rraspec.strip().strip('"'))
            
            
        
    def _updateMonetDB(self, hostPath, clusterNode, metricNode):
        ''' This method updates an RRD file with current metric values. '''
        # If the node has a time stamp then use it to update the RRD.  Otherwise get
        #  the current timestamp.
        cursor = self.connection.cursor()
        processTime = clusterNode.getAttr('localtime')
        if processTime is None:
            processTime = time()
        else:
            processTime = float(processTime)
        try:
            # Update the RRD file with the current timestamp and value
            if metricNode.getAttr('type') == 'float':
               cursor.execute("insert into floats (metric, val, ts, nodename) values ('%s', %f,'%s','%s')" % (metricNode.getAttr('name'), float(metricNode.getAttr('val')), datetime.fromtimestamp(processTime).isoformat(), hostPath))
            elif metricNode.getAttr('type') == 'double':
                cursor.execute("insert into doubles (metric, val, ts, nodename) values ('%s', %f,'%s','%s')" % ( metricNode.getAttr('name'), float(metricNode.getAttr('val')), datetime.fromtimestamp(processTime).isoformat(), hostPath))
            elif metricNode.getAttr('type') == 'uint16':
                 cursor.execute("insert into smallints (metric, val, ts, nodename) values ('%s', %d,'%s','%s')" % ( metricNode.getAttr('name'), int(metricNode.getAttr('val')), datetime.fromtimestamp(processTime).isoformat(), hostPath))
            elif metricNode.getAttr('type') == 'uint32':
                 cursor.execute("insert into ints (metric, val, ts, nodename) values ('%s', %d,'%s','%s')" % ( metricNode.getAttr('name'), int(metricNode.getAttr('val')), datetime.fromtimestamp(processTime).isoformat(), hostPath))
            else:
                 logging.INFO('unable to find type %s' %  metricNode.getAttr('type'))
            self.connection.commit()
            logging.debug('Updated monetdb %s %s with value %s'%(metricNode.getAttr('name'), metricNode.getAttr('type'),str(metricNode.getAttr('val'))))
        except Exception, e:
#        self.connection.rollback()
            logging.info('Error updating monetdb %s %s:%s %s %s - %s'%(hostPath, metricNode.getAttr('name'), `metricNode.getAttr('val')`, metricNode.getAttr('type'), datetime.fromtimestamp(processTime).isoformat(),str(e)))
        cursor.close()

    def start(self):
        '''Called by the engine during initialization to get the plugin going.'''
        #print "RRD start called"
        self.connection =  monetdb.sql.connect(username="monetdb", password="monetdb", hostname="localhost", database="ganglia")
    
    def stop(self):
        '''Called by the engine during shutdown to allow the plugin to shutdown.'''
        #print "RRD stop called"
        self.connection.close()

    def notify(self, clusterNode):
        '''Called by the engine when the internal data source has changed.'''
        # Get the current configuration
        gmetadConfig = getConfig()
        # Find the data source configuration entry that matches the cluster name
        for ds in gmetadConfig[GmetadConfig.DATA_SOURCE]:
            if ds.name == clusterNode.getAttr('name'):
                break
        if ds is None:
            logging.info('No matching data source for %s'%clusterNode.getAttr('name'))
            return
        try:
            if clusterNode.getAttr('status') == 'down':
                return
        except AttributeError:
            pass
        # Create the cluster RRD base path and validate it
        clusterPath = clusterNode.getAttr('name')
        if 'GRID' == clusterNode.id:
            clusterPath = '%s/__SummaryInfo__'%clusterPath

        # We do not want to process grid data
        if 'GRID' == clusterNode.id:
            return

        # Update metrics for each host in the cluster
        for hostNode in clusterNode:
            # Create the host RRD base path and validate it.
            hostPath = '%s/%s'%(clusterPath,hostNode.getAttr('name'))
            # Update metrics for each host
            for metricNode in hostNode:
                # Don't update metrics that are numeric values.
                if metricNode.getAttr('type') in ['string', 'timestamp']:
                    continue
                # Update the MonetDB records.
                self._updateMonetDB(hostPath, clusterNode, metricNode)
        #print "RRD notify called"
