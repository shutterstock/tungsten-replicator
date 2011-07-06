# $Id: ifconfig.rb,v 1.1.1.1 2005/07/02 19:10:57 hobe Exp $
#

require 'ifconfig/common/ifconfig'
require 'ifconfig/linux/network_types'
require 'ifconfig/linux/interface_types'

class Ifconfig
  #
  # Can manually specify the platform (should be output of the 'uname' command)
  # and the ifconfig input
  # 
  def initialize(input=nil,netstat=nil,verbose=nil)
    if input.nil?
      cmd = IO.popen('which ifconfig 2>/dev/null'){ |f| f.readlines[0] }
      if cmd.nil?
        @ifconfig = IO.popen("export LANG=en_US; /sbin/ifconfig -a"){ |f| f.readlines.join }
      else
        cmd.chomp!
        @ifconfig = IO.popen("export LANG=en_US; #{cmd} -a"){ |f| f.readlines.join }
      end
    else
      @ifconfig = input
    end
    @verbose = verbose
    @ifaces = {}
    
    split_interfaces(@ifconfig).each do |iface|
      iface_name = get_iface_name(iface)
      case iface
        when /encap\:ethernet/im
          @ifaces[iface_name] = EthernetAdapter.new(iface_name,iface)
        when /encap\:Local Loopback/im
          @ifaces[iface_name] = LoopbackInterface.new(iface_name,iface)
        when /encap\:IPv6-in-IPv4/im
          @ifaces[iface_name] = IPv6_in_IPv4.new(iface_name,iface)
        when /encap\:Point-to-Point Protocol/im
          @ifaces[iface_name] = PPP.new(iface_name,iface)
        when /encap\:Serial Line IP/im
          @ifaces[iface_name] = SerialLineIP.new(iface_name,iface)
        else
          puts "Unknown Adapter Type on Linux: #{iface}" if @verbose
      end
    end
  end
end
