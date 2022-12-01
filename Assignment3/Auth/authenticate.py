
import collections
import datetime
import time
import csv
from math import sqrt
from polygon import RESTClient
from sqlalchemy import create_engine
from sqlalchemy import text

class Authentication:

    # Init all the necessary variables when instantiating the class
    def __init__(self):
        self.currency_pairs = [["AUD", "USD"],
                               ["GBP", "EUR"],
                               ["USD", "CAD"],
                               ["USD", "JPY"],
                               ["USD", "MXN"],
                               ["EUR", "USD"],
                               ["USD", "CNY"],
                               ["USD", "CZK"],
                               ["USD", "PLN"],
                               ["USD", "INR"]
                               ]
        self.key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"
        self.engine = create_engine("sqlite+pysqlite:///sqlite/final.db", echo=False, future=True)


    # Function slightly modified from polygon sample code to format the date string
    def ts_to_datetime(self, ts) -> str:
        return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    # Function which clears the raw data tables once we have aggregated the data in a 6 minute interval
    def reset_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("DROP TABLE " + curr[0] + curr[1] + "_raw;"))
                conn.execute(text(
                    "CREATE TABLE " + curr[0] + curr[1] + "_raw(ticktime text, fxrate  numeric, inserttime text);"))

    # This creates a table for storing the raw, unaggregated price data for each currency pair in the SQLite database
    def initialize_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text(
                    "CREATE TABLE " + curr[0] + curr[1] + "_raw(ticktime text, fxrate  numeric, inserttime text);"))

    # This creates a table for storing the (6 min interval) aggregated price data for each currency pair in the SQLite database
    def initialize_aggregated_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE "+curr[0]+curr[1]+"_agg(inserttime text, avgfxrate numeric, prev_avg numeric, min_val numeric, max_val numeric, vol_val numeric, fd numeric, return numeric);"))

    # This function is called every 6 minutes to aggregate the data, store it in the aggregate table, and then delete the raw data
    def calc_keltner_bonds(self, volatility, average):
        upper_bounds = []
        lower_bounds = []
        for i in range(100):
            upper_bounds.append(average + (i + 1) * 0.025 * volatility)
            lower_bounds.append(average - (i + 1) * 0.025 * volatility)
        return upper_bounds, lower_bounds


    def aggregate_raw_data_tables(self):
        low_bound_dictionary = collections.defaultdict(list)
        upper_bound_dictionary = collections.defaultdict(list)
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                result = conn.execute(text(
                    "SELECT AVG(fxrate) as avg_price, MAX(fxrate) as max_price, MIN(fxrate) as min_price FROM " + curr[0] + curr[1] + "_raw;"))

                #getting values in 6 minutes
                stats_vals = []
                for row in result:
                    stats_vals.append(row.avg_price)
                    stats_vals.append(row.min_price)
                    stats_vals.append(row.max_price)
                    stats_vals.append(row.max_price - row.min_price)

                #Get the bounds for every currency in 6 minutes
                upper_bounds, lower_bounds = self.calc_keltner_bonds(stats_vals[3], stats_vals[0])

                #get all data in the dictionary
                low_bound_dictionary[curr[0] + curr[1]] = lower_bounds
                upper_bound_dictionary[curr[0] + curr[1]] = upper_bounds

        return low_bound_dictionary, upper_bound_dictionary

    def currencyBuy(self, cost, Units, prev_loss, amount, curr2):
        print("Entered currencyBuy ")
        if amount >= 1 and amount <=100:
            amount = amount - Units
            self.Prev_Action_was_Buy = True
            curr2 += Units* cost 
            print("Bought %d worth (%s). Our current profits and losses in the org currency (%s) : %f." % (Units,self.to,self.from_,(amount-1)))
        else:
            print("There was not enough (%s) to make another buy." % self.from_)
        
        f = open('output1_'+str(self.from_)+str(self.to)+'.csv', 'a', newline='')
        writer = csv.writer(f)
        writer.writerow([Units, amount-1, prev_loss])
        f.close()

    def currencySell(self, cost, Units, prev_loss, amount, curr2):
        if curr2 >= 1 and curr2 <=100:
            amount += Units * (1/cost)
            self.Prev_Action_was_Buy = False
            curr2 -= Units
            print("Sold %d worth of the target currency (%s). Our current profits and losses in the original currency (%s) are: %f." % (Units,self.to,self.from_,(amount-1)))
        else:
            print("There was not enough  (%s) to make another sell." % self.to)   
        
        f = open('output1_'+str(self.from_)+str(self.to)+'.csv', 'a', newline='')
        writer = csv.writer(f)
        writer.writerow([Units, amount-1, prev_loss])
        f.close()


    def executeData(self, iteration, lower_bounds, upper_bounds, outputFileName,count):
        #start the connections
        with self.engine.begin() as conn:
            file = open(outputFileName, 'a')
            writer = csv.writer(file)

            for curr in self.currency_pairs:
                key = curr[0] + curr[1]
                result = conn.execute(text("SELECT fxrate from " + key + "_raw;"))
                result_stat = conn.execute(text("SELECT AVG(fxrate) as avg_price, MAX(fxrate) as max_price, MIN(fxrate) as min_price FROM " + key + "_raw;"))
                return_fx = 0
                             
                # for every bound, check how many data points will cross it
                count = 0
                for i in range(100):
                    # iterate through each row and check if it passes the current bound
                    for row in result:
                        if upper_bounds[key][i] <= row.fxrate or lower_bounds[key][i] >= row.fxrate:
                            # if crossed, increment
                            count += 1

                    #for every bound, we check for every data point to check if they violate
                for row in result_stat:
                    max_price = row.max_price
                    avg_price = row.avg_price
                    min_price = row.min_price
                    volatility = row.max_price - row.min_price
                    fd = count
                    if volatility != 0:
                        fd = count/volatility

                if count > 120:

                    prev_count = conn.execute(text("SELECT COUNT(prev_avg) as ccnt FROM "+curr[0]+curr[1]+"_agg;"))
                    for cn in prev_count:
                        prev_count_value = cn.ccnt

                    last_date = 0
                    date_res = conn.execute(text("SELECT MAX(ticktime) as last_date FROM "+curr[0]+curr[1]+"_raw;"))
                    for row in date_res:
                        last_date = row.last_date
                    
                    avg_prev = conn.execute(text("SELECT prev_avg FROM "+curr[0]+curr[1]+"_agg LIMIT "+str(prev_count_value)+"-10, 10;"))
                    for vls1 in avg_prev:
                        avg_prev_value = vls1.prev_avg


                # writing the data into arg table for furture reference
                conn.execute(text("INSERT INTO "+curr[0]+curr[1]+"_agg(inserttime, avgfxrate , prev_avg, min_val, max_val, vol_val, fd, return) VALUES (:inserttime, :avgfxrate, , :prev_avg, :min_val, :max_val, :vol_val, :fd, :return);"),
                [{"inserttime": last_date, "avgfxrate": avg_price,  "prev_avg": avg_prev_value, "min_val": min_price, "max_val": max_price, "vol_val": volatility, "fd": fd, "return": return_fx}])

                hour_One = 360
            
                # At each hour we will call this block of code
                if count%hour_One == 0:
                    Total_hour_spent = count//hour_One
                    print("Hours spent: ", Total_hour_spent)

                    if count <= hour_One:
                        avg_ret = 0
                        if curr[4] == -1: 
                            units_sell = 100
                            amount = units_sell
                            curr2 = units_sell
                            curr[3].currencySell(avg_prev_value, units_sell, avg_ret, amount, curr2)
                        if curr[4] == 1:
                            units_buy = 100
                            amount = units_buy
                            curr2 = units_buy
                            curr[3].currencyBuy(avg_prev_value, units_buy, avg_ret, amount, curr2)

                if count > hour_One:
                    # getting the table count
                    value_get = conn.execute(text("SELECT COUNT(return) as c_value FROM AUDUSD_agg;"))
                    for cn in value_get:
                        cnts1 = cn.c_value
                    
                    #Get previous 10 return values
                    prev_return = conn.execute(text("SELECT return as p_return_value FROM AUDUSD_agg LIMIT "+str(cnts1)+"-10, 10;"))
                    prev_return_all = []
                    for each in prev_return:
                        prev_return_all.append(each.p_return_value)
                    
                    #Getting the average value
                    avg_of_all_returns = sum(prev_return_all)

                    
                    # losses for each cycle of 260 seconds
                    losses = [0.250, 0.150, 0.100, 0.050, 0.050, 0.050, 0.050, 0.050, 0.050, 0.050]
                    
                    Total_losses = losses[(count//hour_One)-1]
                    
                    if abs(avg_of_all_returns) <= Total_losses: 

                        if curr[4] == 0:
                            print("Trade is done for the day")

                        if curr[4] == 1:
                            num_to_buy = 100*(1+(count//t1))
                            amount = num_to_buy
                            curr2 = num_to_buy
                            curr[3].currencyBuy(avg_prev_value, num_to_buy, avg_of_all_returns, amount, curr2)

                        if curr[4] == -1:
                            num_to_sell = 100*(1+(count//t1))
                            amount = num_to_sell
                            curr2 = num_to_sell
                            curr[3].currencySell(avg_prev_value, num_to_sell, avg_of_all_returns, amount, curr2)
                    else:
                        curr[4] = 0         

    def getData(self, outputFileName):
        # Number of list iterations - each one should last about 1 second
        count = 0
        agg_count = 0

        # initaltion of iterations
        iteration = 0
        return_fx = 0
        # Create the needed tables in the database
        self.initialize_raw_data_tables()
        self.initialize_aggregated_tables()
        # Open a RESTClient for making the api calls
        client = RESTClient(self.key)
        # Loop that runs until the total duration of the program hits 24 hours.
        previous_lower_bounds, previous_upper_bounds = [], []
        while count < 86400:  # 86400 seconds = 24 hours
            # print(count, "Seconds over")
            # Make a check to see if 6 minutes has been reached or not
            if agg_count == 360:
                # aggregate and get upper and lower bounds
                lower_bounds, upper_bounds = self.aggregate_raw_data_tables()
                # print(lower_bounds, upper_bounds)
                # in the first iteration, we cannot calculate violations. So, if iteration is zero, just take bounds and skip.
                if iteration == 0:
                    previous_lower_bounds = lower_bounds
                    previous_upper_bounds = upper_bounds
                    self.reset_raw_data_tables()
                    agg_count = 0
                    return_fx = 0
                else:
                    # from the second iteration,
                    self.executeData(iteration + 1, previous_lower_bounds, previous_upper_bounds, outputFileName)
                    previous_lower_bounds = lower_bounds
                    previous_upper_bounds = upper_bounds
                    self.reset_raw_data_tables()
                    agg_count = 0

                iteration += 1
                print(iteration, " - Iteration completed.")
                print(iteration)

            # Only call the api every 1 second, so wait here for 0.75 seconds, because the code takes about .15 seconds to run
            time.sleep(0.75)
            # Increment the counters
            count += 1
            agg_count += 1
            # Loop through each currency pair
            for currency in self.currency_pairs:
                # Set the input variables to the API
                from_ = currency[0]
                to = currency[1]
                # Call the API with the required parameters
                try:
                    resp = client.get_real_time_currency_conversion(from_, to, amount=100, precision=2)
                except:
                    continue
                # This gets the Last Trade object defined in the API Resource
                last_trade = resp.last
                # print(type(last_trade), last_trade.timestamp)
                # Format the timestamp from the result
                dt = self.ts_to_datetime(last_trade.timestamp)
                # Get the current time and format it
                insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Calculate the price by taking the average of the bid and ask prices
                avg_price = (last_trade.bid + last_trade.ask) / 2
                # Write the data to the SQLite database, raw data tables
                with self.engine.begin() as conn:
                    conn.execute(text(
                        "INSERT INTO " + from_ + to + "_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, :inserttime)"),
                                 [{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])
