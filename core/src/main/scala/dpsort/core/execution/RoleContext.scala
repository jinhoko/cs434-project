package dpsort.core.execution

trait RoleContext {
  def initialize : Unit
  def execute : Unit
  def terminate : Unit
}
