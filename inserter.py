import asyncio
import datetime
import logging
import random
import sys
import time

from random import getrandbits
from ipaddress import IPv4Network, IPv4Address,ip_address,ip_network,ip_interface


import asyncpool
from aioch import Client

import inserter_config
from utils import format_date_from_timestamp

total_inserted_events = 0
total_rows = 0
pool = {}


def create_pool():
    for i in range(inserter_config.WORKERS):
        client = Client(inserter_config.HOST)
        pool[i] = {"c": client, "a": True}


def get_connection():
    for i, val in pool.items():
        if val['a']:
            val['a'] = False
            return i, val['c']
def gen_model_key(a,b):
    global total_inserted_events
    object_key = str(total_inserted_events)+ str(b)
    m_key = '12345.'+ str(a)+ '.'+ object_key
    return m_key
def gen_ip():
    subnet = IPv4Network("10.0.0.0/19") 
    bits = getrandbits(subnet.max_prefixlen - subnet.prefixlen)
    addr = IPv4Address(subnet.network_address + bits)
    addr_str = str(addr)
    return addr_str

def generate_random_event() -> dict:
    #flow_model_key = gen_model_key(515)
    f_protocol = random.choice(['TCP','UDP'])
    traffic_type = random.choice(['EAST_WEST_TRAFFIC', 'INTERNET_TRAFFIC'])
    port_num = random.choice([443,53,80,135,2055,412,76,3001,2099,11029,1998,1995,2019])
    if port_num == 443:
        port_name = 'https'
    elif port_num == 53:
        port_name = 'dns'
    elif port_num == 80:
        port_name = 'https'
    elif port_num == 135:
        port_name = 'epmap'
    else:
        port_name = 'other'
    if port_name == 'dns':
        f_protocol = 'UDP'
        traffic_type = 'EAST_WEST_TRAFFIC'
    port_name_num = '[' + str(port_num)+ ']'+ port_name 
    src_ip = gen_ip()
    dst_ip = gen_ip()
    src_ip_cidr = src_ip + '/27'
    dst_ip_cidr = dst_ip + '/27'
    src_cidr = str(ip_interface(src_ip_cidr).network)
    src_network = src_cidr[:-4]
    src_netmask = str(ip_interface(src_ip_cidr).netmask)
    start_src_ip_int = int(ip_network(src_ip_cidr, strict=False)[0])
    end_src_ip_int = int(ip_network(src_ip_cidr, strict=False)[-1])
    dst_cidr = str(ip_interface(dst_ip_cidr).network)
    dst_network = dst_cidr[:-4]
    dst_netmask = str(ip_interface(dst_ip_cidr).netmask)
    start_dst_ip_int = int(ip_network(dst_ip_cidr, strict=False)[0])
    end_dst_ip_int = int(ip_network(dst_ip_cidr, strict=False)[-1])

    if (ip_address(dst_ip) in ip_network(src_ip_cidr, strict=False)):
        network_layer = 'LAYER_2'
    else:
        network_layer = 'LAYER_3'
    VM_range = range(0,9999)
    src_VM_num = random.choice(VM_range)
    src_VM_name = 'VM' + str(src_VM_num)
    src_host_num = src_VM_num//20
    src_host_name = 'HOST' + str(src_host_num)
    src_cluster_num = src_host_num//10
    src_cluster_name = 'CLUSTER' + str(src_cluster_num)
    src_Nsx_vc_num = src_cluster_num//10
    src_Nsx_manager_name = 'NSX' + str(src_Nsx_vc_num)
    src_VC_name = 'VC' + str(src_Nsx_vc_num)
    DC_name = 'DATACENTER101'
    VM_range.remove(src_VM_num)
    dst_VM_num = random.choice(0,9999)
    dst_VM_name = 'VM' + str(dst_VM_num)
    dst_host_num = dst_VM_num//20
    dst_host_name = 'HOST' + str(dst_host_num)
    dst_cluster_num = dst_host_num//10
    dst_cluster_name = 'CLUSTER' + str(dst_cluster_num)
    dst_Nsx_vc_num = dst_cluster_num//10
    dst_Nsx_manager_name = 'NSX' + str(dst_Nsx_vc_num)
    dst_VC_name = 'VC' + str(dst_Nsx_vc_num)
    DC_name = 'DATACENTER101'
    flow_name = src_ip + 'VM' + str(src_VM_num) + dst_ip
    flow_tag_list = ['TAG_TRAFFIC_TYPE_UNKNOWN' , 'TAG_INTERNET_TRAFFIC' ,'TAG_EAST_WEST_TRAFFIC' , 'TAG_VM_VM_TRAFFIC', 'TAG_VM_PHY_TRAFFIC' , 'TAG_PHY_PHY_TRAFFIC' , 'TAG_SRC_IP_VMKNIC' ,'TAG_DST_IP_VMKNIC' , 'TAG_SRC_IP_ROUTER_INT' , 'TAG_DST_IP_ROUTER_INT', 'TAG_SRC_IP_VM','TAG_DST_IP_VM', 'TAG_SRC_IP_INTERNET','TAG_DST_IP_INTERNET', 'TAG_SRC_IP_PHYSICAL', 'TAG_DST_IP_PHYSICAL', 'TAG_SAME_HOST' , 'TAG_DIFF_HOST' , 'TAG_COMMON_HOST_INFO_UNKNOWN' , 'TAG_SHARED_SERVICE' , 'TAG_NOT_SHARED_SERVICE' , 'TAG_NETWORK_SWITCHED' , 'TAG_NETWORK_ROUTED' , 'TAG_NETWORK_UNKNOWN' , 'TAG_SRC_IP_VTEP' , 'TAG_DST_IP_VTEP' , 'TAG_UNICAST' , 'TAG_BROADCAST' , 'TAG_MULTICAST' , 'TAG_SRC_IP_LINK_LOCAL' , 'TAG_DST_IP_LINK_LOCAL' , 'TAG_SRC_IP_CLASS_E' , 'TAG_DST_IP_CLASS_E' , 'TAG_SRC_IP_CLASS_A_RESERVED' , 'TAG_DST_IP_CLASS_A_RESERVED' , 'TAG_INVALID_IP_PACKETS' , 'TAG_NOT_ANALYZED' , 'TAG_GENERIC_INTERNET_SRC_IP' , 'TAG_SNAT_DNAT_FLOW' , 'TAG_NATTED' , 'TAG_MULTINICS' , 'TAG_MULTI_NATRULE' , 'TAG_SRC_VC' , 'TAG_DST_VC' , 'TAG_SRC_AWS' , 'TAG_DST_AWS' , 'TAG_WITHIN_DC' , 'TAG_DIFF_DC' , 'TAG_SRC_IP_IN_SNAT_RULE_TARGET' , 'TAG_DST_IP_IN_DNAT_RULE_ORIGINAL' , 'TAG_SRC_IP_MULTI_NAT_RULE' , 'TAG_DNAT_IP_MULTI_NAT_RULE' , 'DEPRECATED_TAG_DST_IP_IN_SNAT_RULE_TARGET' , 'DEPRECATED_TAG_SRC_IP_IN_DNAT_RULE_ORIGINAL' , 'TAG_INVALID_IP_DOMAIN' , 'TAG_WITHIN_VPC' , 'TAG_DIFF_VPC' , 'TAG_SRC_IP_IN_MULTIPLE_SUBNETS' , 'TAG_DST_IP_IN_MULTIPLE_SUBNETS' , 'TAG_SFLOW' , 'TAG_PRE_NAT_FLOW' , 'TAG_POST_NAT_FLOW' , 'TAG_LOGICAL_FLOW' , 'TAG_SRC_K8S_POD' , 'TAG_DST_K8S_POD' , 'TAG_POD_POD_TRAFFIC' , 'TAG_VM_POD_TRAFFIC' , 'TAG_POD_PHYSICAL_TRAFFIC']
    new_list = random.sample(flow_tag_list,5)
    reported_action = random.choice(['ALLOW' , 'DENY' , 'DROP', 'REJECT' , 'REDIRECT' , 'DONT_REDIRECT'])
    rule_num = random.randint(1,8888)
    ruleid = 'RULE'+str(rule_num)



    return_value = {
        "name": flow_name,
        "modelkey_cid" : 12345,
        "modelkey_otype": 515,
        "modelkey_oid" : total_inserted_events,
        "port.fstart": [port_num],
        "port.fend": [port_num],
        "port.display": [port_num],
        "port.ianaName": [port_name],
        "port.ianaPortDisplay": [port_name_num],
        "fProtocol": f_protocol,
        "srcIP.prefixLength": [27],
        "srcIP.ipAddress": [gen_ip()],
        "srcIP.netMask": [src_netmask],
        "srcIP.networkAddress": [src_network],
        "srcIP.cidr": [src_cidr],
        "srcIP.fstart": [start_src_ip_int],
        "srcIP.fend": [end_src_ip_int],
        "srcIP.ipaddresstype": ['SUBNET'],
        "srcIP.privateaddress": [1],
        "srcIP.Source": ['UNKNOWN'],
        "srcIP.Ipmetadata_domain": ['UNKNOWN'],
        "srcIP.Ipmetadata_isp": ['UNKNOWN'],
        "dstIP.prefixLength": [27],
        "dstIP.ipAddress": [gen_ip()],
        "dstIP.netMask": [dst_netmask],
        "dstIP.networkAddress": [dst_network],
        "dstIP.cidr": [dst_cidr],
        "dstIP.fstart": [start_dst_ip_int],
        "dstIP.fend": [end_dst_ip_int],
        "dstIP.ipaddresstype": ['SUBNET'],
        "dstIP.privateaddress": [1],
        "dstIP.Source": ['UNKNOWN'],
        "dstIP.Ipmetadata_domain": ['UNKNOWN'],
        "dstIP.Ipmetadata_isp": ['UNKNOWN'],
        "TrfficType": traffic_type,
        "shared": [1],
        "networkLayer": network_layer,
        "srcsubnet.prefixLength": [19],
        "srcsubnet.ipAddress": [src_ip + '/19'],
        "srcsubnet.netMask": [str(ip_interface(src_ip + '/19').netmask)],
        "srcsubnet.networkAddress": [str(ip_interface(src_ip + '/19').network)[:-4]],
        "srcsubnet.cidr": [str(ip_interface(src_ip + '/19').network)],
        "srcsubnet.fstart": [int(ip_network(src_ip + '/19', strict=False)[0])],
        "srcsubnet.fend": [int(ip_network(src_ip + '/19', strict=False)[-1])],
        "srcsubnet.ipaddresstype": ['SUBNET'],
        "srcsubnet.privateaddress": [1],
        "srcsubnet.Source": ['UNKNOWN'],
        "srcsubnet.Ipmetadata_domain": ['UNKNOWM'],
        "srcsubnet.Ipmetadata_isp": ['UNKNOWM'],
        "dstsubnet.prefixLength": [19],
        "dstsubnet.ipAddress": [dst_ip + '/19'],
        "dstsubnet.netMask": [str(ip_interface(dst_ip + '/19').netmask)],
        "dstsubnet.networkAddress": [str(ip_interface(dst_ip + '/19').network)[:-4]],
        "dstsubnet.cidr": [str(ip_interface(dst_ip + '/19').network)],
        "dstsubnet.fstart": [int(ip_network(dst_ip + '/19', strict=False)[0])],
        "dstsubnet.fend": [int(ip_network(dst_ip + '/19', strict=False)[-1])],
        "dstsubnet.ipaddresstype": ['SUBNET'],
        "dstsubnet.privateaddress": [1],
        "dstsubnet.Source": ['UNKNOWN'],
        "dstsubnet.Ipmetadata_domain": ['UNKNOWM'],
        "dstsubnet.Ipmetadata_isp": ['UNKNOWM'],
        "withinhost": int(src_host_num == dst_host_num),
        "typetag": new_list,
        "__searchTags": [flow_name, src_VM_name, dst_VM_name],
        "__related_entities": ['flows','traffic'],
        "srcVmTags": ['admin@vmware.com','badri@vmware.com'],
        "dstVmTags": ['vamsi@vmware.com','krishna@vmware.com'],
        "attribute.reportedaction": [reported_action],
        "attribute.reportedRuleId": [ruleid],
        "attribute.collectorId": [total_inserted_events//8],
        "attribute_rule.name": 
        "attribute_rule.modelkey_oid": [gen_model_key(random.choice([7,8,603,612,663,917,5200]))],
        "attribute_firewallmanager.name":
        "attribute_firewallmanager.modelkey_oid": [gen_model_key(random.choice([7,8,612]))],
        "activedpIds": total_inserted_events//8,
        "typeTagsPacked": 'KCC0IABACBAA',
        "protectionStatus": random.choice(['UNKNOWN_STATUS', 'PROTECTED', 'ANY_ANY','UN_PROTECTED']),
        "flowAction": 'ALLOW',
        "srcDnsInfo.ipDomain":
        "srcDnsInfo.ip":
        "srcDnsInfo.domainName":
        "srcDnsInfo.hostName":
        "srcDnsInfo.source":
        "dstDnsInfo.ipDomain":
        "dstDnsInfo.ip":
        "dstDnsInfo.domainName":
        "dstDnsInfo.hostName":
        "dstDnsInfo.source":
        "reporterEntity.collectorId": [total_inserted_events//8],
        "reporterEntity_reporter.name":
        "reporterEntity_reporter.modelkey_oid": [gen_model_key(random.choice([7,8,603,612,663,917,5200]))],
        "SchemaVersion":
        "lastActivity":
        "activity":
        "srck8Info.k8scollectorId":
        "srck8Info_k8sservice.name":
        "srck8Info_k8sservice.modelkey_oid": [gen_model_key(1504)],
        "srck8Info_k8scluster.name":
        "srck8Info_k8scluster.modelkey_oid": [gen_model_key(1501)],
        "srck8Info_k8snamespace.name":
        "srck8Info_k8snamespace.modelkey_oid": [gen_model_key(1503)],
        "srck8Info_k8snode.name":
        "srck8Info_k8snode.modelkey_oid": [gen_model_key(1502)],
        "dstk8Info.k8scollectorId":
        "dstk8Info_k8sservice.name":
        "dstk8Info_k8sservice.modelkey_oid": [gen_model_key(1504)],
        "dstk8Info_k8scluster.name":
        "dstk8Info_k8scluster.modelkey_oid": [gen_model_key(1501)],
        "dstk8Info_k8snamespace.name":
        "dstk8Info_k8snamespace.modelkey_oid": [gen_model_key(1503)],
        "dstk8Info_k8snode.name":
        "dstk8Info_k8snode.modelkey_oid": [gen_model_key(1502)],
        "lbflowtype":
        "loadBalancerInfo.type":
        "loadBalancerInfo.collectorId":
        "loadBalancerInfo.relevantPort":
        "loadBalancerInfo_loadbalancervIP.prefixLength":
        "loadBalancerInfo_loadbalancervIP.ipAddress":
        "loadBalancerInfo_loadbalancervIP.netMask":
        "loadBalancerInfo_loadbalancervIP.networkAddress":
        "loadBalancerInfo_loadbalancervIP.cidr":
        "loadBalancerInfo_loadbalancervIP.fstart":
        "loadBalancerInfo_loadbalancervIP.fend":
        "loadBalancerInfo_loadbalancervIP.ipaddresstype":
        "loadBalancerInfo_loadbalancervIP.privateaddress":
        "loadBalancerInfo_loadbalancervIP.Source":
        "loadBalancerInfo_loadbalancervIP.Ipmetadata_domain":
        "loadBalancerInfo_loadbalancervIP.Ipmetadata_isp":
        "loadBalancerInfo_loadbalancerinternalIP.prefixLength":
        "loadBalancerInfo_loadbalancerinternalIP.ipAddress":
        "loadBalancerInfo_loadbalancerinternalIP.netMask":
        "loadBalancerInfo_loadbalancerinternalIP.networkAddress":
        "loadBalancerInfo_loadbalancerinternalIP.cidr":
        "loadBalancerInfo_loadbalancerinternalIP.fstart":
        "loadBalancerInfo_loadbalancerinternalIP.fend":
        "loadBalancerInfo_loadbalancerinternalIP.ipaddresstype":
        "loadBalancerInfo_loadbalancerinternalIP.privateaddress":
        "loadBalancerInfo_loadbalancerinternalIP.Source":
        "loadBalancerInfo_loadbalancerinternalIP.Ipmetadata_domain":
        "loadBalancerInfo_loadbalancerinternalIP.Ipmetadata_isp":
        "srcIpEntity.name": 
        "srcIpEntity.modelkey_oid": [gen_model_key(541)],
        "dstIpEntity.name":
        "dstIpEntity.modelkey_oid": [gen_model_key(541)],
        "srcNic.name": 
        "srcNic.modelkey_oid": [gen_model_key(random.choice([18,17,16]))],
        "dstNic.name":
        "dstNic.modelkey_oid": [gen_model_key(random.choice([18,17,16]))],
        "srcVm.name": [src_VM_name],
        "srcVm.modelkey_oid": [gen_model_key(1601)],
        "dstVm.name": [dst_VM_name],
        "dstVm.modelkey_oid": [gen_model_key(1601)],
        "srcSg.name": 
        "srcSg.modelkey_oid": [gen_model_key(random.choice([550,213,602,958]))],
        "dstSg.name":
        "dstSg.modelkey_oid": [gen_model_key(random.choice([550,213,602,958]))],
        "srcIpSet.name":
        "srcIpSet.modelkey_oid": [gen_model_key(random.choice([84,214]))]
        "dstIpSet.name":
        "dstIpSet.modelkey_oid": [gen_model_key(random.choice([84,214]))]
        "srcSt.name":
        "srcSt.modelkey_oid":
        "dstSt.name":
        "dstSt.modelkey_oid":
        "dstSt.name":
        "dstSt.modelkey_oid":
        "srcL2Net.name":
        "srcL2Net.modelkey_oid":
        "dstL2Net.name":
        "dstL2Net.modelkey_oid":
        "srcGroup.name":
        "srcGroup.modelkey_oid":
        "dstGroup.name":
        "dstGroup.modelkey_oid":
        "srcCluster.name": [src_cluster_name],
        "srcCluster.modelkey_oid":
        "dstCluster.name": [dst_cluster_name],
        "dstCluster.modelkey_oid":
        "srcRp.name": 
        "srcRp.modelkey_oid":
        "dstRp.name":
        "dstRp.modelkey_oid":
        "srcDc.name": [DC_name],
        "srcDc.modelkey_oid":
        "dstDc.name": [DC_name],
        "dstDc.modelkey_oid":
        "srcHost.name": [src_host_name],
        "srcHost.modelkey_oid":
        "dstHost.name": [dst_host_name],
        "dstHost.modelkey_oid":
        "srcManagers.name": [src_Nsx_manager_name]
        "srcManagers.modelkey_oid":
        "dstManagers.name":[dst_Nsx_manager_name],
        "dstManagers.modelkey_oid": 
        "flowDomain.name":
        "flowDomain.modelkey_oid":
        "srcLookupDomain.name":
        "srcLookupDomain.modelkey_oid":
        "dstLookupDomain.name":
        "dstLookupDomain.modelkey_oid":
        "srcVpc.name":
        "srcVpc.modelkey_oid":
        "dstVpc.name":
        "dstVpc.modelkey_oid":
        "srcTransportNode.name":
        "srcTransportNode.modelkey_oid":
        "dstTransportNode.name":
        "dstTransportNode.modelkey_oid":
        "srcDvpg.name":
        "srcDvpg.modelkey_oid":
        "dstDvpg.name":
        "dstDvpg.modelkey_oid":
        "srcDvs.name":
        "srcDvs.modelkey_oid":
        "dstDvs.name":
        "dstDvs.modelkey_oid":
    }

    return return_value


async def write_to_event(data: list, _):
    global total_inserted_events
    print(f"writing to {inserter_config.TABLE_NAME} {len(data)} rows; total count: {total_inserted_events}")
    conn_id, conn = get_connection()
    await conn.execute(f'INSERT INTO {inserter_config.DB_NAME}.{inserter_config.TABLE_NAME} VALUES', data)

    total_inserted_events += len(data)

    return_connection(conn_id)


def return_connection(connection_id):
    pool[connection_id]['a'] = True


def generate_random_events(number_events: int) -> list:
    return [generate_random_event() for _ in range(number_events)]


async def fill_events(_loop, number_per_day, bulk_size):
    async with asyncpool.AsyncPool(
            _loop,
            num_workers=inserter_config.WORKERS,
            worker_co=write_to_event,
            max_task_time=300,
            log_every_n=10,
            name="CHPool",
            logger=logging.getLogger("CHPool")) as p:

        #insert_time = datetime.datetime(2018, 1, 1)
        for i in range(365):
            for _ in range(int(number_per_day / bulk_size)):
                events = generate_random_events(bulk_size)
                await p.push(events, None)

            insert_time = insert_time + datetime.timedelta(days=1)


def log_experiment(experiment_took):
    with open("inserter_experiments_log.txt", 'a') as f:
        text = f"{format_date_from_timestamp(time.time())} - " \
            f"table: {inserter_config.TABLE_NAME} - " \
            f"db: {inserter_config.DB_NAME} - " \
            f"inserted: {total_inserted_events} - " \
            f"took: {round(experiment_took, 2)} - " \
            f"bulk size: {inserter_config.BULK_SIZE} - " \
            f"events per day: {inserter_config.EVENTS_PER_DAY} - " \
            f"workers: {inserter_config.WORKERS}\n"
        f.write(text)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    create_pool()

    start = time.time()
    print("inserter started at", format_date_from_timestamp(start))
    loop.run_until_complete(fill_events(loop, inserter_config.EVENTS_PER_DAY, inserter_config.BULK_SIZE))
    end = time.time()
    took = end - start
    print(f"inserter ended at {format_date_from_timestamp(end)}; took: {took} seconds")
    log_experiment(took)