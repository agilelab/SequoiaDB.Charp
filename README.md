# SequoiaDB.Charp
C# Driver For SequoiaDB 

//create Sequoiadb
Sequoiadb sdb = new Sequoiadb("192.168.23.57:50000");
sdb.Connect("", "");

//GetCollecitonSpace。
var cs = sdb.GetCollecitonSpace("dbo");

//GetCollection。
var coll = cs.GetCollection<HFareDetail>();

//insert from rdb。
List<HFareDetail> vList =null;
using (AgileHIS.Entities.DbEntities db = new AgileHIS.Entities.DbEntities())
{
    vList = db.HFareDetails.ToList();
    //insert。
    foreach (var item in vList)
    {
        coll.Insert(item);
    }
    System.Console.WriteLine(string.Format("insert {0} records", vList.Count));
    System.Console.ReadLine();
}

//update docuemnt by linq。
var v1 = vList.FirstOrDefault();
v1.Name = string.Empty;
v1.Cash = decimal.Zero;
coll.Update(v1, p => p.ID == v1.ID);
//update docuemnt by linq。
coll.Update(p => new HFareDetail { Cash = decimal.Zero, Name = string.Empty, Price = decimal.Zero }, p => p.ChargeTime >DateTime.Now.AddDays(-1));
//delete by linq
coll.Delete(p => p.ChargeTime > DateTime.Now.AddDays(-1));

//get Count
int count = coll.AsQueryable<HFareDetail>()
    .Where(p => p.SourceID==0)
    .Count();

//Linq query Take\Skip。
var vList2 = coll.AsQueryable<HFareDetail>()
    .Where(p => p.CreateTime > DateTime.Now.Date.AddMonths(-12))
    .Skip(10).Take(1000)
    .ToList();
System.Console.WriteLine(string.Format("query {0} records", vList.Count));

//Linq FirstOrDefault。
var vFare = coll.AsQueryable<HFareDetail>()
    .Where(p => p.CreateTime > DateTime.Now.Date.AddMonths(-12))
    .FirstOrDefault();
System.Console.WriteLine(vFare);

//Linq aggregate，Not Implemented
var sum = coll.AsQueryable<HFareDetail>()
    .Where(p => p.CreateTime > DateTime.Now.Date.AddMonths(-12))
    .Sum(p => p.Cash);
