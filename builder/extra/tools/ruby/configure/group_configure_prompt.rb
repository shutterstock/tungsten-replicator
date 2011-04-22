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
  
  def initialize(name, prompt, singular, plural)
    @group_prompts = []
    @name = name.to_s()
    @prompt = prompt.to_s()
    @config = nil
    @weight = 0
    @singular = singular.to_s().downcase()
    @plural = plural.to_s().downcase()
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
    unless enabled?()
      save_disabled_value()
      return
    end
    
    description = get_description()
    unless description == nil
      puts
      Configurator.instance.write_divider
      puts description
      puts
    end
    
    # Do we want to collect default values?
    #each_prompt{
    #  |prompt|
    #  unless prompt.allow_group_default()
    #    next
    #  end
    #  
    #  prompt.run()
    #}
    
    each_member{
      |member|
      
      puts @prompt.sub('@value', member)
      each_prompt{
        |prompt|
        
        prompt.set_member(member)
        prompt.run()
      }
      puts
    }
    
    # Loop over the group until the user does not specify a new alias or
    # triggers one of the keywords
    while true
      new_alias = nil

      puts "Enter an alias for the next #{@singular}.  Enter nothing to stop entering #{@plural}."
      while new_alias == nil
        new_alias = input_value("New #{@singular} alias", "")
        case new_alias
        when COMMAND_HELP
          puts
          puts get_help()
          puts
        when COMMAND_PREVIOUS
          # Go back
          raise ConfigurePreviousPrompt
        when COMMAND_ACCEPT_DEFAULTS
          # Accept the default value for this and all remaining prompts
          raise ConfigureAcceptAllDefaults
        when COMMAND_SAVE
          # Save the current config values and exit
          raise ConfigureSaveConfigAndExit
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
      
      each_prompt{
        |prompt|
        prompt.set_member(new_alias)
        prompt.run();
      }
      puts
    end
    
    puts "#{@singular.capitalize()} information defined for #{get_members().join(', ')}"
  end
  
  def save_current_value
    each_member_prompt{
      |member, prompt|
      
      prompt.save_current_value()
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
    errors = []
    
    each_member_prompt{
      |member, prompt|
      
      begin
        prompt.is_valid?()
      rescue ConfigurePromptError => e
        errors << e
      end
    }
    
    if errors.length > 0
      raise ConfigurePromptErrorSet.new(errors)
    else
      true
    end
  end
  
  # Collect the full list of keys that are allowed in the config file
  def get_keys
    keys = []
    
    each_member_prompt{
      |member, prompt|
      
      if prompt.is_enabled?()
        keys << prompt.get_name()
      end
    }
    
    keys
  end

  # Add a single prompt to this group
  def add_prompt(new_prompt)
    unless new_prompt.is_a?(GroupConfigurePromptMember)
      raise "Unable to add #{new_prompt.class().name()}:#{new_prompt.get_name()} because it does not extend GroupConfigurePromptMember"
    end
    
    unless new_prompt.is_a?(ConfigurePrompt)
      raise "Unable to add #{new_prompt.class().name()}:#{new_prompt.get_name()} because it does not extend ConfigurePrompt"
    end
    
    new_prompt.set_group(self)
    @group_prompts << new_prompt
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
  end
  
  # Loop over each member-prompt combination to execute &block
  # This will exclude the defaults entry in the group
  def each_member_prompt(&block)
    each_member{
      |member|
      each_prompt{
        |prompt|
        prompt.set_member(member)
        block.call(member, prompt)
      }
    }
  end
end

module GroupConfigurePromptMember
  # Assign the parent group to this prompt
  def set_group(val)
    @parent_group = val
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
  def get_value
    @config.getPropertyOr([@parent_group.name, get_member(), @name], get_default_value())
  end
  
  # Does this prompt support a group-wide default value to be specified
  def allow_group_default
    false
  end
  
  # Build the help filename based on the basic config key
  def get_prompt_help_filename()
    "#{get_interface_text_directory()}/help_#{@name}"
  end
  
  # Build the description filename based on the basic config key
  def get_prompt_description_filename()
    "#{get_interface_text_directory()}/prompt_#{@name}"
  end
end