defmodule Boltx.Routing.Transaction do
  @moduledoc """
  An open transaction of `Boltx.Routing.Pool.transaction/3`, on one leader
  connection. Pass it to `Boltx.Routing.Pool.query/4` and
  `Boltx.Routing.Pool.rollback/2`; it is only valid inside the function.
  """

  defstruct [:conn, :address, :database, :ref]

  @type t :: %__MODULE__{
          conn: DBConnection.t(),
          address: String.t(),
          database: String.t() | nil,
          ref: reference()
        }
end
