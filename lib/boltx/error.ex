defmodule Boltx.Error do
  @moduledoc """
  Error handling for Boltx.

  This module defines error types and mappings for Neo4j errors,
  including cluster-specific errors that require special handling.
  """

  # Standard errors
  @error_map %{
    "Neo.ClientError.Security.Unauthorized" => :unauthorized,
    "Neo.ClientError.Request.Invalid" => :request_invalid,
    "Neo.ClientError.Statement.SemanticError" => :semantic_error,
    "Neo.ClientError.Statement.SyntaxError" => :syntax_error,
    # Cluster errors
    "Neo.ClientError.Cluster.NotALeader" => :not_a_leader,
    "Neo.ClientError.Cluster.Forbidden" => :cluster_forbidden,
    "Neo.TransientError.Cluster.NotALeader" => :not_a_leader,
    "Neo.TransientError.Cluster.FeatureUnavailable" => :cluster_feature_unavailable,
    "Neo.TransientError.Cluster.UnavailableClusterMember" => :cluster_member_unavailable,
    # Database errors
    "Neo.ClientError.Database.DatabaseNotFound" => :database_not_found,
    "Neo.ClientError.Statement.ForbiddenOnReadOnlyDatabase" => :forbidden_on_read_only_database,
    "Neo.TransientError.General.DatabaseUnavailable" => :database_unavailable
  }

  # Errors that indicate the query should be retried on a different server
  @retryable_cluster_errors [
    :not_a_leader,
    :cluster_member_unavailable,
    :forbidden_on_read_only_database
  ]

  # Errors that indicate the server should be removed from writers list
  @write_failure_errors [
    :not_a_leader,
    :forbidden_on_read_only_database
  ]

  @type t() :: %__MODULE__{
          module: module(),
          code: atom(),
          bolt: %{code: binary(), message: binary() | nil} | nil,
          packstream: %{bits: any() | nil} | nil
        }

  defexception [:module, :code, :bolt, :packstream]

  @spec wrap(module(), atom()) :: t()
  def wrap(module, code) when is_atom(code), do: %__MODULE__{module: module, code: code}

  @spec wrap(module(), binary()) :: t()
  def wrap(module, code) when is_binary(code), do: wrap(module, to_atom(code))

  @spec wrap(module(), map()) :: t()
  def wrap(module, bolt_error) when is_map(bolt_error),
    do: %__MODULE__{module: module, code: bolt_error.code |> to_atom(), bolt: bolt_error}

  def wrap(module, code, packstream),
    do: %__MODULE__{module: module, code: code, packstream: packstream}

  @doc """
  Return the code for the given error.

  ### Examples

       iex> {:error, %Boltx.Error{} = error} = do_something()
       iex> Exception.message(error)
       "Unable to perform this action."


  """
  @spec message(t()) :: String.t()
  def message(%__MODULE__{code: code, module: module} = error) do
    if code == :syntax_error do
      """
      Syntax error
      #{error.bolt.message}
      """
    else
      inspect(error, pretty: true)
    end
    # TODO: move to module.format_error(code) later
  end

  @doc """
  Gets the corresponding atom based on the error code.
  """
  @spec to_atom(String.t()) :: atom()
  def to_atom(error_message) do
    Map.get(@error_map, error_message, :unknown)
  end

  @doc """
  Checks if the error is a retryable cluster error.

  Retryable cluster errors indicate that the query should be retried
  on a different server in the cluster.
  """
  @spec retryable_cluster_error?(t()) :: boolean()
  def retryable_cluster_error?(%__MODULE__{code: code}) do
    code in @retryable_cluster_errors
  end

  def retryable_cluster_error?(_), do: false

  @doc """
  Checks if the error indicates a write failure that should
  result in the server being removed from the writers list.
  """
  @spec write_failure_error?(t()) :: boolean()
  def write_failure_error?(%__MODULE__{code: code}) do
    code in @write_failure_errors
  end

  def write_failure_error?(_), do: false

  @doc """
  Checks if the error is a NotALeader error.
  """
  @spec not_a_leader?(t()) :: boolean()
  def not_a_leader?(%__MODULE__{code: :not_a_leader}), do: true
  def not_a_leader?(_), do: false

  @doc """
  Checks if the error is a transient error that might succeed on retry.
  """
  @spec transient_error?(t()) :: boolean()
  def transient_error?(%__MODULE__{bolt: %{code: code}}) when is_binary(code) do
    String.contains?(code, "TransientError")
  end

  def transient_error?(_), do: false
end
