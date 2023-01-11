import org.wasmer.Instance
import org.wasmer.Memory

object InstanceWrapper {
  private var instance: Option[Instance] = None

  def get: Instance = instance.get

  def set(instance: Instance): Unit = {
    InstanceWrapper.instance = Option(instance)
  }
}