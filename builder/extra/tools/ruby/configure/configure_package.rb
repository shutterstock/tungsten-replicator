class ConfigurePackage
  include ConfigureMessages
  
  def initialize(config)
    @config = config
  end
  
  def store_config_file?
    true
  end
  
  def get_non_interactive_prompts
    []
  end
  
  def get_prompts
    []
  end
  
  def get_validation_checks
    []
  end
  
  def parsed_options?(arguments)
    true
  end
  
  def post_prompt_handler_run
    true
  end
  
  def prepare_saved_config(config)
    config
  end
  
  def output_usage
    puts "Usage: configure [general-options]"
    output_general_usage()
  end
  
  def output_general_usage
    Configurator.instance.write_divider(Logger::ERROR)
    puts "General options:"
    puts "-a, --advanced        Enable advanced options"
    puts "-b, --batch           Execute the configuration without interactive prompts"
    puts "-c, --config file     Sets name of config file (default: tungsten.cfg)"
    puts "-h, --help            Displays help message"
    puts "-q, --quiet           Only display error messages"
    puts "-v, --verbose         Display all messages"
    puts "--no-validation       Skip all validation checks"
    puts "--validate-only       Skip all deployment steps"
    puts "--property=key=value  Set the property key to the value in any file that is modified by the configure script"
  end
  
  def output_usage_line(argument, msg = "", default = nil, max_line = 70)
    if default.to_s() != ""
      if msg != ""
        msg += " "
      end
      
      msg += "[#{default}]"
    end
    
    if argument.length > 28 || (argument.length + msg.length > max_line)
      puts argument
      
      words = msg.split(' ')
      
      force_add_word = true
      line = format("%-29s", " ")
      while words.length() > 0
        if !force_add_word && line.length() + words[0].length() > max_line
          puts line
          line = format("%-29s", " ")
          force_add_word = true
        else
          line += " " + words.shift()
          force_add_word = false
        end
      end
      puts line
      
    else
      puts format("%-29s", argument) + " " + msg
    end
  end
end