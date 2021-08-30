sealed trait Color
final case class Red(v: Int) extends Color
final case class Green(v: Int) extends Color
final case class Blue(v: Int) extends Color

final class ColorPalette[A <: Color, B <: Color] private (val color: A)(fun: A => B) {
  def mixitUp[C <: Color](mixer: B => C): ColorPalette[A, C] = 
    ColorPalette(color)(fun andThen mixer)
                                                  
  def show: Color =
    fun(color)
}

object ColorPalette {
  def apply[A <: Color, B <: Color](color: A)(fun: A => B = identity[A] _): ColorPalette[A, B] =
    new ColorPalette(color)(fun)
  
  //def apply[A <: Color](color: A): ColorPalette[A, A] =
  //  new ColorPalette(color)(identity)
}

ColorPalette(Red(2))().mixitUp(c => Blue(c.v)).show
