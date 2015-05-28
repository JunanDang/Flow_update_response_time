import threading
import time
import datetime

from random import randint
from operator import attrgetter

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import tcp
from ryu.lib.packet import ipv4
from ryu import utils

class SimpleSwitch13(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SimpleSwitch13, self).__init__(*args, **kwargs)
        self.cookie = 0
        flowrate = input('Enter flowmode rate:')
        self.flowrate = flowrate
        dp = input('Enter 1 (pica8-1) or 2 (pica8-2):')
        if dp == 1:
            self.dpid = 7461418321859182785
        elif dp == 2:
            self.dpid = 6790944927334400710

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        msg = ev.msg
        datapath = ev.msg.datapath
        if datapath.id != self.dpid:
            return
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        self.logger.info('OFPSwitchFeatures datapath_id=0x%016x n_buffers=%d n_tables=%d auxiliary_id=%d capabilities=0x%08x' % (msg.datapath_id, msg.n_buffers, msg.n_tables,msg.auxiliary_id, msg.capabilities))

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

        d = threading.Thread(target=self.dummy_entries, args=(datapath,))
        m = threading.Thread(target=self.monitor, args=(datapath,))
        d.start()
        m.start()

    def add_flow(self, datapath, priority, match, actions):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        self.cookie += 1

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
                                                   # cookie
        mod = parser.OFPFlowMod(datapath=datapath, cookie=self.cookie, priority=priority,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

    def dummy_entries(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        while self.cookie <= 1500:
            for i in range(0,self.flowrate):
                src_ip = '111.111.111.111'
                dst_ip = '111.111.111.112'
                src_port = self.cookie
                dst_port = self.cookie + 2000
                priority = randint(10,8000)
                # Adding flow entries
                actions = [parser.OFPActionOutput(88)]
                matchs = parser.OFPMatch(eth_type=0x0800, ip_proto=6, tcp_src=src_port, tcp_dst=dst_port, ipv4_src=src_ip, ipv4_dst=dst_ip)
                self.add_flow(datapath, priority, matchs, actions)

            datapath.send_barrier()
            time.sleep(1)

    def monitor(self, datapath):
        time.sleep(0.2)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        while True:
            req = parser.OFPTableStatsRequest(datapath)
            datapath.send_msg(req)
            print time.time(), 'request cookie#:', self.cookie

            time.sleep(1)

    @set_ev_cls(ofp_event.EventOFPTableStatsReply, MAIN_DISPATCHER)
    def table_stats_reply_handler(self, ev):
        active_count = 0
        for stat in ev.msg.body:
            active_count += stat.active_count
        print time.time(), 'flow_count', active_count



