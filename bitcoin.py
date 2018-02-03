
def is_interval_border(x, levels):
   for level in levels:
      if x[0].hour in [level[0], level[1]]:
         return True

   return False
