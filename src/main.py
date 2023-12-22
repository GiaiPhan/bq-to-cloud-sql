from ca_utils import MySQL




if __name__=="__main__":
    mysql_object = MySQL(host="34.124.221.251", user="userdemo", 
                 password="userdemo@?C&_:cNvt}{P(%;1", database="spotonchain_demo")

    print(mysql_object)