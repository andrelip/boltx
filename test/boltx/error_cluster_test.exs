defmodule Boltx.ErrorClusterTest do
  use ExUnit.Case, async: true

  alias Boltx.Error

  describe "cluster error handling" do
    test "to_atom/1 maps NotALeader error codes" do
      assert Error.to_atom("Neo.ClientError.Cluster.NotALeader") == :not_a_leader
      assert Error.to_atom("Neo.TransientError.Cluster.NotALeader") == :not_a_leader
    end

    test "to_atom/1 maps other cluster error codes" do
      assert Error.to_atom("Neo.ClientError.Cluster.Forbidden") == :cluster_forbidden
      assert Error.to_atom("Neo.TransientError.Cluster.FeatureUnavailable") == :cluster_feature_unavailable
      assert Error.to_atom("Neo.TransientError.Cluster.UnavailableClusterMember") == :cluster_member_unavailable
    end

    test "to_atom/1 returns :unknown for unmapped codes" do
      assert Error.to_atom("Neo.SomeOther.Error") == :unknown
    end

    test "not_a_leader?/1 returns true for NotALeader errors" do
      error = %Error{code: :not_a_leader, module: __MODULE__}
      assert Error.not_a_leader?(error)
    end

    test "not_a_leader?/1 returns false for other errors" do
      error = %Error{code: :syntax_error, module: __MODULE__}
      refute Error.not_a_leader?(error)
    end

    test "retryable_cluster_error?/1 returns true for retryable errors" do
      assert Error.retryable_cluster_error?(%Error{code: :not_a_leader, module: __MODULE__})
      assert Error.retryable_cluster_error?(%Error{code: :cluster_member_unavailable, module: __MODULE__})
      assert Error.retryable_cluster_error?(%Error{code: :forbidden_on_read_only_database, module: __MODULE__})
    end

    test "retryable_cluster_error?/1 returns false for non-retryable errors" do
      refute Error.retryable_cluster_error?(%Error{code: :syntax_error, module: __MODULE__})
      refute Error.retryable_cluster_error?(%Error{code: :unauthorized, module: __MODULE__})
    end

    test "write_failure_error?/1 returns true for write failure errors" do
      assert Error.write_failure_error?(%Error{code: :not_a_leader, module: __MODULE__})
      assert Error.write_failure_error?(%Error{code: :forbidden_on_read_only_database, module: __MODULE__})
    end

    test "write_failure_error?/1 returns false for other errors" do
      refute Error.write_failure_error?(%Error{code: :syntax_error, module: __MODULE__})
    end

    test "transient_error?/1 returns true for transient errors" do
      error = %Error{
        code: :not_a_leader,
        module: __MODULE__,
        bolt: %{code: "Neo.TransientError.Cluster.NotALeader", message: nil}
      }
      assert Error.transient_error?(error)
    end

    test "transient_error?/1 returns false for client errors" do
      error = %Error{
        code: :not_a_leader,
        module: __MODULE__,
        bolt: %{code: "Neo.ClientError.Cluster.NotALeader", message: nil}
      }
      refute Error.transient_error?(error)
    end
  end
end
