defmodule MySQLOTPTest do
  @moduledoc """
  Test for MySQL/OTP (Erlang native MySQL client)
  Uses the :mysql Erlang library
  """

  def main(_args \\ []) do
    IO.puts("Starting MySQL/OTP Test")
    
    cli_args = Burrito.Util.Args.get_arguments()
    IO.puts("Received CLI args: #{inspect(cli_args)}")
    
    result = run(cli_args)
    System.halt(0)
    result
  end

  defp run(args) do
    if length(args) < 3 do
      IO.puts("Usage: mysql-otp-test <user> <port> <database>")
      System.halt(1)
    end
    
    user = Enum.at(args, 0)
    port_str = Enum.at(args, 1)
    database = Enum.at(args, 2)
    
    {port, _} = Integer.parse(port_str)

    # Start MySQL/OTP connection using Erlang :mysql module
    {:ok, pid} = :mysql.start_link([
      host: '127.0.0.1',
      port: port,
      user: String.to_charlist(user),
      password: '',
      database: String.to_charlist(database)
    ])

    IO.puts("Connected using MySQL/OTP (Erlang connector)")

    # Test queries
    queries = [
      "create table test (pk int, `value` int, primary key(pk))",
      "describe test",
      "select * from test",
      "insert into test (pk, `value`) values (0,0)",
      "select * from test",
      "call dolt_add('-A')",
      "call dolt_commit('-m', 'my commit')",
      "select COUNT(*) FROM dolt_log",
      "call dolt_checkout('-b', 'mybranch')",
      "insert into test (pk, `value`) values (1,1)",
      "call dolt_commit('-a', '-m', 'my commit2')",
      "call dolt_checkout('main')",
      "call dolt_merge('mybranch')",
      "select COUNT(*) FROM dolt_log"
    ]

    # Execute each query
    Enum.each(queries, fn query ->
      IO.puts("Executing: #{query}")
      
      case :mysql.query(pid, query) do
        :ok ->
          IO.puts("  → OK")
          
        {:ok, column_names, rows} ->
          IO.puts("  → #{length(rows)} row(s) returned with #{length(column_names)} column(s)")
          
        {:error, reason} ->
          IO.puts("Query failed: #{query}")
          IO.puts("Error: #{inspect(reason)}")
          :mysql.stop(pid)
          System.halt(1)
      end
    end)

    # Verify final log count
    {:ok, _columns, rows} = :mysql.query(pid, "select COUNT(*) FROM dolt_log")
    [[count]] = rows
    
    if count != 3 do
      IO.puts("Expected 3 commits in dolt_log, got #{count}")
      :mysql.stop(pid)
      System.halt(1)
    end

    :mysql.stop(pid)
    IO.puts("\nAll MySQL/OTP tests passed!")
    :ok
  end
end

