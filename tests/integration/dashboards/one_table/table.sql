SELECT
  Address,
  City,
  State,
  `Zip Code`,
  Country
FROM
VALUES
  ('160 Spear St 15th Floor', 'San Francisco', 'CA', '94105', 'USA'),
  ('756 W Peachtree St NW, Suite 03W114', 'Atlanta', 'GA', '30308', 'USA'),
  ('500 108th Ave NE, Suite 1820', 'Bellevue', 'WA', '98004', 'USA'),
  ('125 High St, Suite 220', 'Boston', 'MA', '02110', 'USA'),
  ('2120 University Ave, Suite 722', 'Berkeley', 'CA', '94704', 'USA') AS tab(Address, City, State, `Zip Code`, Country)
