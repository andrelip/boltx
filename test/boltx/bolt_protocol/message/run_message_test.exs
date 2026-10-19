defmodule Boltx.BoltProtocol.Message.RunMessageTest do
  use ExUnit.Case, async: true

  alias Boltx.BoltProtocol.Message.RunMessage
  alias Boltx.BoltProtocol.MessageEncoder

  @tag :core
  test "inside a transaction RUN carries an empty extra: BEGIN set the rest" do
    assert RunMessage.encode(5.0, "RETURN 1", %{}, %{in_tx: true}) ==
             MessageEncoder.encode(0x10, ["RETURN 1", %{}, %{}])
  end
end
