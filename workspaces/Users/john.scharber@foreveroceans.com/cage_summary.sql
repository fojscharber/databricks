SELECT
  cage.siteid,
  cageid,
  cage.designation as label,
  capunittype.designation as type,
  cage.netvolume,
  cage.cagevolume,
  cage.surface,
  cage.isvirtual,
  cage.cr,
  cage.cx,
  cage.cy,
  cage.cz
from
  cage,
  capunittype
where
  capunittype.capunittypeid == cage.capunittypeid
