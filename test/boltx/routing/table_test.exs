defmodule Boltx.Routing.TableTest do
  use ExUnit.Case, async: true

  alias Boltx.Routing.Table

  describe "new/1" do
    test "creates a new routing table with defaults" do
      table = Table.new("neo4j")

      assert table.database == "neo4j"
      assert table.routers == []
      assert table.readers == []
      assert table.writers == []
      assert table.ttl == 300
      assert table.last_updated_at != nil
    end
  end

  describe "parse/2" do
    test "parses routing info from server response" do
      routing_info = %{
        "servers" => [
          %{"addresses" => ["server1:7687"], "role" => "WRITE"},
          %{"addresses" => ["server2:7687", "server3:7687"], "role" => "READ"},
          %{"addresses" => ["server1:7687", "server2:7687", "server3:7687"], "role" => "ROUTE"}
        ],
        "ttl" => 60
      }

      table = Table.parse(routing_info, "mydb")

      assert table.database == "mydb"
      assert table.writers == ["server1:7687"]
      assert table.readers == ["server2:7687", "server3:7687"]
      assert table.routers == ["server1:7687", "server2:7687", "server3:7687"]
      assert table.ttl == 60
      assert table.initialized_without_writers == false
    end

    test "handles missing writers" do
      routing_info = %{
        "servers" => [
          %{"addresses" => ["server2:7687"], "role" => "READ"},
          %{"addresses" => ["server2:7687"], "role" => "ROUTE"}
        ],
        "ttl" => 30
      }

      table = Table.parse(routing_info)

      assert table.writers == []
      assert table.initialized_without_writers == true
    end
  end

  describe "is_fresh?/2" do
    test "returns false for empty table" do
      table = %Table{database: nil, last_updated_at: nil}

      refute Table.is_fresh?(table, :read)
      refute Table.is_fresh?(table, :write)
    end

    test "returns true for fresh table with appropriate servers" do
      table = %Table{
        database: nil,
        routers: ["router:7687"],
        readers: ["reader:7687"],
        writers: ["writer:7687"],
        ttl: 300,
        last_updated_at: System.monotonic_time(:second)
      }

      assert Table.is_fresh?(table, :read)
      assert Table.is_fresh?(table, :write)
    end

    test "returns false when no readers for read mode" do
      table = %Table{
        database: nil,
        routers: ["router:7687"],
        readers: [],
        writers: ["writer:7687"],
        ttl: 300,
        last_updated_at: System.monotonic_time(:second)
      }

      refute Table.is_fresh?(table, :read)
    end

    test "returns false when no writers for write mode" do
      table = %Table{
        database: nil,
        routers: ["router:7687"],
        readers: ["reader:7687"],
        writers: [],
        ttl: 300,
        last_updated_at: System.monotonic_time(:second)
      }

      refute Table.is_fresh?(table, :write)
    end
  end

  describe "remove_writer/2" do
    test "removes a writer from the list" do
      table = %Table{
        database: nil,
        writers: ["writer1:7687", "writer2:7687"]
      }

      updated = Table.remove_writer(table, "writer1:7687")

      assert updated.writers == ["writer2:7687"]
    end
  end

  describe "deactivate_server/2" do
    test "removes server from all lists" do
      table = %Table{
        database: nil,
        routers: ["server:7687", "other:7687"],
        readers: ["server:7687", "other:7687"],
        writers: ["server:7687"]
      }

      updated = Table.deactivate_server(table, "server:7687")

      assert updated.routers == ["other:7687"]
      assert updated.readers == ["other:7687"]
      assert updated.writers == []
    end
  end

  describe "all_servers/1" do
    test "returns unique list of all servers" do
      table = %Table{
        database: nil,
        routers: ["server1:7687", "server2:7687"],
        readers: ["server2:7687", "server3:7687"],
        writers: ["server1:7687"]
      }

      servers = Table.all_servers(table)

      assert Enum.sort(servers) == ["server1:7687", "server2:7687", "server3:7687"]
    end
  end
end
