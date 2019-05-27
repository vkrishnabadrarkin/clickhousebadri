from random import getrandbits
from ipaddress import IPv4Network, IPv4Address,ip_address,ip_network,ip_interface
subnet = IPv4Network("10.0.0.0/19") 
bits = getrandbits(subnet.max_prefixlen - subnet.prefixlen)
addr = IPv4Address(subnet.network_address + bits)
addr_str = str(addr)
print(addr_str)
subnet2 = IPv4Network("10.0.0.0/19") 
bits = getrandbits(subnet2.max_prefixlen - subnet2.prefixlen)
addr2 = IPv4Address(subnet2.network_address + bits)
addr_str2 = str(addr2)
print(int(ip_address('10.166.17.91')))
print(addr_str2)
addddddr = addr_str2 + '/32'
k = str(ip_interface(addddddr).netmask)[:-4]
print(k)
if (ip_address(addr_str) in ip_network(addddddr, strict=False)):
    print('hi')
else:
    print ('ji')
print(int(ip_network(addddddr, strict=False)[0]))
print(int(ip_network(addddddr, strict=False)[-1]))
