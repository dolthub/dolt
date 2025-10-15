defmodule Simple.Application do
  use Application

  @impl true
  def start(_type, _args) do
    IO.puts("Simple.Application.start/2 called")
    
    # This is a CLI app, so we run the command and exit
    # Start the application supervisor first
    children = []
    opts = [strategy: :one_for_one, name: Simple.Supervisor]
    
    {:ok, pid} = Supervisor.start_link(children, opts)
    IO.puts("Supervisor started")
    
    # Spawn a task to run the CLI command
    IO.puts("Spawning SmokeTest.main/1")
    spawn(fn ->
      SmokeTest.main([])
    end)
    
    {:ok, pid}
  end
end

