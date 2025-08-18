using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;

namespace SproutDB.Engine.Tests;

public class Dummy { }

/*

get users 
  follow users.id -> orders.user_id as orders 
  where orders.total > 100 
  group by users.name 
  having count(orders) > 2 
  order by count(orders) desc 
  select users.name, count(orders) as order_count, sum(orders.total) as total_spent
  page 1 size 10

get users 
  follow users.id -> orders.user_id as orders 
  where orders.total > 100 
  group by users.name 
  having count(orders) > 2 
  order by count(orders) desc 
  select users.name, count(orders) as order_count, sum(orders.total) as total_spent
  page 1 size 10

*/