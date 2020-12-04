package dpsort.core.execution

trait Role {
  def initialize : Unit
  def execute : Unit
  def terminate : Unit
}
