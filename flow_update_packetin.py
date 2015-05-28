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
        self.packetin = 0
        self.query_map = {}
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

        m = threading.Thread(target=self.monitor, args=(datapath,))
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

    def send_packet_out(self, datapath, msg, in_port):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        # Send packet out
        actions = [parser.OFPActionOutput(5-in_port)]

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        self.packetin += 1
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        pkt_ip = pkt.get_protocol(ipv4.ipv4)#Get the ipv4 packet
        
        if not pkt_ip:
            self.send_packet_out(datapath,msg,in_port)
            return
        src_ip = pkt_ip.src
        dst_ip = pkt_ip.dst

        key = src_ip+dst_ip
        #if not the fisrt packet of the flow, don't query CAB
        if key in self.query_map:
            self.send_packet_out(datapath,msg,in_port)
            return
        self.query_map[key] = time.time()

        priority = randint(10,8000)
        actions = [parser.OFPActionOutput(5-in_port)]
        matchs = parser.OFPMatch(eth_type=0x0800, ip_proto=6, ipv4_src=src_ip, ipv4_dst=dst_ip)
        self.add_flow(datapath, priority, matchs, actions)

        # Send packet out
        self.send_packet_out(datapath,msg,in_port)


    def monitor(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        while True:
            req = parser.OFPTableStatsRequest(datapath)
            datapath.send_msg(req)
            print time.time(), 'request cookie#:', self.cookie

            time.sleep(10)

    @set_ev_cls(ofp_event.EventOFPTableStatsReply, MAIN_DISPATCHER)
    def table_stats_reply_handler(self, ev):
        active_count = 0
        for stat in ev.msg.body:
            active_count += stat.active_count
        print time.time(), 'flow_count', active_count



