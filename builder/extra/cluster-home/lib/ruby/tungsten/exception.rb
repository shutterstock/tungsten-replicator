#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# 
# Provide definition of different error types. 
#

# Denotes a system error, e.g. from a bug. 
class SystemError < StandardError
end

# Denotes a user error, e.g., from a bad option. 
class UserError < StandardError
end

# Denotes a runtime error, e.g., inability to write a file or run a program. 
class ExecError < StandardError
end
