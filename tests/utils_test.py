
def test_strip():
  from pyAccess import utils as pAutils
  for txt in [' test', 'test ', ' test ']:
    assert(pAutils.strip(' test')) == 'test'

