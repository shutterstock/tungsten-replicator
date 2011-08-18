#
# This class collects GroupConfigurePromptMember classes and repeats them to
# create a hierarchy of prompt information.  The group name is used as the 
# top level value in the config and the alias entered by the user is the 
# second level.  The third level in the config is defined by the prompt 
# object.
#
class GroupConfigurePrompt
  include ConfigurePromptInterface
  attr_accessor :name, :singular, :plural
  
  def initialize(name, prompt, singular, plural, template_prefix)
    @group_prompts = []
    @name = name.to_s()
    @prompt = prompt.to_s()
    @config = nil
    @weight = 0
    @singular = singular.to_s().downcase()
    @plural = plural.to_s().downcase()
    @template_prefix = template_prefix
    
    @prompt_pairs = nil
    @previous_prompts = []
    @last_run_prompt_pair_i = 0
  end
  
  # The config object must be set down on each of the prompts so that they
  # have direct access
  def set_config(config)
    @config = config
    each_prompt{
      |prompt|
      prompt.set_config(config)
    }
  end
  
  def is_initialized?
    (get_name() != "" && get_prompts().size() > 0)
  end
  
  def run
    # Skip this prompt and remove the config value if this prompt isn't needed
    unless enabled_for_config?()
      save_disabled_value()
    end
    
    unless enabled?
      return
    end
    
    description = get_description()
    unless description == nil
      puts
      Configurator.instance.write_divider
      puts description
      puts
    end
    
    unless @prompt_pairs != nil
      @prompt_pairs = []

      # Do we want to collect default values?
      #each_prompt{
      #  |prompt|
      #  unless prompt.allow_group_default()
      #    next
      #  end
      #  
      #  @prompt_pairs << [DEFAULTS, prompt]
      #}
    
      delete_prompt = prepare_prompt(DeleteGroupMemberPrompt.new())
      each_member{
        |member|
        @prompt_pairs << [member, delete_prompt]
        each_prompt{
          |prompt|
          @prompt_pairs << [member, prompt]
        }
      }
    end
    
    previous_prompt = @previous_prompts.pop()
    unless previous_prompt == nil
      @last_run_prompt_pair_i = previous_prompt
    end
    
    run_pairs()
    
    # Loop over the group until the user does not specify a new alias or
    # triggers one of the keywords
    while can_add_member()
      new_alias = default_member_alias(get_members().size())

      if new_alias != nil
        puts "Enter an alias for the next #{@singular}.  Enter nothing to stop entering #{@plural}."
      end
      
      new_alias_prompt = TemporaryPrompt.new("New #{@singular} alias")
      while new_alias == nil
        new_alias = new_alias_prompt.run()

        case new_alias
        when DEFAULTS
          error("You may not use '#{DEFAULTS} as an alias'")
          new_alias = nil
        else
          unless new_alias == "" || new_alias =~ /^[a-zA-Z0-9_]+$/
            error("The new alias must consist only of letters, digits, and underscore (_)")
            new_alias = nil
          end
          
          if get_members.include?(new_alias)
            error("'#{new_alias}' is already being used as an alias")
            new_alias = nil
          end
        end
      end
      
      # Exit the while true loop
      break if new_alias.to_s() == ""
      
      # Place prompts into the stack for the new member
      each_prompt{
        |prompt|
        @prompt_pairs << [new_alias, prompt]
      }
      
      run_pairs()
    end
    
    puts
    puts "#{@singular.capitalize()} information defined for #{get_members().join(', ')}"
  end
  
  # Run through the prompt_pairs list using last_prompt_pair_i as the starting point
  def run_pairs()
    prev_i = nil
    
    while @last_run_prompt_pair_i < @prompt_pairs.length()
      i = @last_run_prompt_pair_i
      begin
        member = @prompt_pairs[i][0]
        curr_prompt = @prompt_pairs[i][1]
        if prev_i == nil || (@prompt_pairs[prev_i][0] != member)
          puts ""
          puts @prompt.sub('@value', member)
        end
        prev_i = i
        
        Configurator.instance.debug("Start prompt #{curr_prompt.class().name()}:#{curr_prompt.get_name()}")
        curr_prompt.set_member(member)
        curr_prompt.run()
        Configurator.instance.debug("Finish prompt #{curr_prompt.class().name()}:#{curr_prompt.get_name()}")
        if curr_prompt.allow_previous?()
          @previous_prompts.push(i)
        end
        
        @last_run_prompt_pair_i += 1
      rescue DeleteGroupMember
        @prompt_pairs.delete_if{
          |item|
          (item[0] == member)
        }
        @config.setProperty([@name, member], nil)
        prev_i = nil
      rescue ConfigurePreviousPrompt
        previous_prompt = @previous_prompts.pop()
        if previous_prompt == nil
          raise ConfigurePreviousPrompt
        else
          @last_run_prompt_pair_i = previous_prompt
        end
      end
    end
  end
  
  def save_current_value
    each_member_prompt{
      |member, prompt|
      
      if prompt.enabled?
        prompt.save_current_value()
      else
        prompt.save_disabled_value()
      end
    }
  end
  
  def save_disabled_value
    each_member_prompt{
      |member, prompt|
      
      prompt.save_disabled_value()
    }
  end
  
  # Validate each of the prompts across all of the defined members
  def is_valid?
    each_member_prompt{
      |member, prompt|
      
      prompt.is_valid?()
    }
    
    true
  end
  
  def can_add_member
    true
  end
  
  def default_member_alias(member_index)
    nil
  end
  
  # Collect the full list of keys that are allowed in the config file
  def get_keys
    keys = []
    
    each_member_prompt{
      |member, prompt|
      
      if prompt.enabled_for_config?() || (prompt.get_disabled_value() != nil)
        keys << prompt.get_name()
      end
    }
    
    keys
  end

  # Add a single prompt to this group
  def add_prompt(prompt)
    unless prompt.is_a?(ConfigurePrompt)
      raise "Unable to add #{prompt.class().name()}:#{prompt.get_name()} because it does not extend ConfigurePrompt"
    end
    
    prompt = prepare_prompt(prompt)
    @group_prompts << prompt
  end
  
  def prepare_prompt(prompt)
    unless prompt.is_a?(GroupConfigurePromptMember)
      prompt.extend(GroupConfigurePromptMember)
    end
    
    prompt.set_group(self)
    
    if @config != nil
      prompt.set_config(@config)
    end
    
    prompt
  end
  
  # Add a list of prompts to this group
  # self.add_prompts(prompt1, prompt2, prompt3)
  def add_prompts(*new_prompts)
    new_prompts_count = new_prompts.size
    for i in 0..(new_prompts_count-1)
      add_prompt(new_prompts[i])
    end
  end
  
  # Get the list of prompts in this group
  def get_prompts
    @group_prompts || []
  end
  
  # Get the list of members excluding the defaults entry
  def get_members
    (@config.getPropertyOr(@name, {}).keys() - [DEFAULTS])
  end
  
  # Loop over each member to execute &block
  # This will exclude the defaults entry in the group
  def each_member(&block)
    get_members().each{
      |member|
      
      block.call(member)
    }
  end
  
  # Loop over each prompt to execute &block
  def each_prompt(&block)
    get_prompts().each{
      |prompt|
      
      block.call(prompt)
    }
    
    self
  end
  
  # Loop over each member-prompt combination to execute &block
  # This will exclude the defaults entry in the group
  def each_member_prompt(&block)
    errors = []
    
    each_member{
      |member|
      each_prompt{
        |prompt|
        begin
          prompt.set_member(member)
          block.call(member, prompt)
        rescue ConfigurePromptError => cpe
          errors << cpe
        rescue => e
          begin
            val = prompt.get_value()
          rescue
            val = ""
          end
          
          dup_prompt = prompt.dup()
          prepare_prompt(dup_prompt)
          errors << ConfigurePromptError.new(dup_prompt, e.message, val)
        end
      }
    }
    
    if errors.length > 0
      raise ConfigurePromptErrorSet.new(errors)
    else
      true
    end
  end
  
  def output_config_file_usage
    puts ""
    output_usage_line(@name + ".<alias>", @prompt)
    each_prompt{
      |p|
      p.output_config_file_usage()
    }
  end
  
  def output_template_file_usage
    puts ""
    output_usage_line(@template_prefix)
    each_prompt{
      |p|
      if p.enabled_for_template_file?()
        p.output_template_file_usage()
      end
    }
  end
  
  def get_property(attrs, allow_disabled = false)
    if attrs[0] != @name
      raise IgnoreError
    end
    
    if attrs.size == 1
      return @config.getNestedProperty(attrs)
    end
    
    if attrs.size == 2
      return @config.getNestedProperty(attrs)
    end
    
    each_prompt{
      |prompt|
      
      begin
        prompt.set_member(attrs[1])
        value = prompt.get_property(attrs.slice(2, attrs.length), allow_disabled)
        
        return value
      rescue IgnoreError
        #Do Nothing
      end
    }
    
    raise IgnoreError
  end
  
  def find_template_value(attrs, transform_values_method)
    if attrs[0] != @name
      raise IgnoreError
    end
    
    if attrs.size == 1
      return @config.getNestedProperty(attrs)
    end
    
    if attrs.size == 2
      return @config.getNestedProperty(attrs)
    end
    
    each_prompt{
      |prompt|
      
      begin
        prompt.set_member(attrs[1])
        value = prompt.find_template_value(attrs.slice(2, attrs.length), transform_values_method)
        
        return value
      rescue IgnoreError
        #Do Nothing
      end
    }
    
    raise IgnoreError
  end
  
  def update_deprecated_keys()
    each_member_prompt{
      |member, prompt|
      
      prompt.update_deprecated_keys()
    }
  end
end

module GroupConfigurePromptMember
  # Assign the parent group to this prompt
  def set_group(val)
    @parent_group = val
  end
  
  def get_group
    @parent_group
  end
  
  # Assign the current member for this prompt
  def set_member(member_name)
    @member_name = member_name
  end
  
  # Reset the member assignment for this prompt
  def clear_member
    @member_name = nil
  end
  
  # Return the current member or the defaults member if none is set
  def get_member
    if @member_name
      @member_name
    else
      DEFAULTS
    end
  end
  
  # Return the name with the full hierarchy included
  def get_name
    "#{@parent_group.name}.#{get_member()}.#{@name}"
  end
  
  # Get an array prepared for the Properties.*Property calls
  def get_member_key(name = nil)
    if name == nil
      name = @name
    end
    
    [@parent_group.name, get_member(), name]
  end
  
  # Get the prompt text with the member prefixed to display
  def get_display_prompt
    "#{@parent_group.singular.capitalize} #{get_member()}: #{get_prompt()}"
  end
  
  # Get the value for this member-property pair from the current config 
  # or the default value if none is found
  def get_value(allow_default = true, allow_disabled = false)
    value = @config.getProperty(get_name())
    if value == nil && allow_default && (enabled_for_config?() || allow_disabled)
      global_default = ConfigurePrompt.get_global_default(@name)
      
      if global_default == nil
        value = get_default_value()
      else
        begin
          value = accept?(global_default)
        rescue
          value = global_default
        end
      end
    end
    
    value
  end
  
  # Does this prompt support a group-wide default value to be specified
  def allow_group_default
    true
  end
  
  # Build the help filename based on the basic config key
  def get_prompt_help_filename()
    "#{get_interface_text_directory()}/help_#{@name}"
  end
  
  # Build the description filename based on the basic config key
  def get_prompt_description_filename()
    "#{get_interface_text_directory()}/prompt_#{@name}"
  end
  
  def get_config_file_usage_symbol
    "  ." + @name
  end
  
  def get_template_file_usage_symbol
    "  ." + Configurator.instance.get_constant_symbol(@name)
  end
end

class DeleteGroupMemberPrompt < TemporaryPrompt
  include GroupConfigurePromptMember
  
  def initialize
    super("Do you want to remove this member from the configuration?")
  end
  
  def get_prompt
    "Do you want to remove this #{@parent_group.singular} from the configuration?"
  end
  
  def run
    value = super()
    
    if value != nil && value.downcase().strip() =~ /^(true|yes|y)$/
      puts "#{@parent_group.singular.capitalize} #{get_member()}: Removing this #{@parent_group.singular}"
      raise DeleteGroupMember
    end
  end
  
  def enabled?
    super() && @config.getProperty([@parent_group.name, get_member()]) != nil
  end
  
  def get_value(allow_default = true, allow_disabled = false)
    "no"
  end
end

class DeleteGroupMember < StandardError
end