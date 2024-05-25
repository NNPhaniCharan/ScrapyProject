# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector


class BookscraperPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != "description":
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()
        for val in ['category', 'product_type']:
            value = adapter.get(val)
            adapter[val] = value.lower()
        for val in ['price_excl_tax', 'price_incl_tax', 'tax','price']:
            value = adapter.get(val)
            adapter[val] = float(value.replace('£', '0'))

        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        num_rev_str = adapter.get('num_reviews')
        adapter[val] = int(num_rev_str)

        stars_string = adapter.get('stars')
        split_stars_array = stars_string.split(' ')
        stars_text_value = split_stars_array[1].lower()

        # Mapping from text to numbers
        stars_mapping = {
            "zero": 0,
            "one": 1,
            "two": 2,
            "three": 3,
            "four": 4,
            "five": 5
        }

        # Convert text to number using the mapping
        adapter['stars'] = stars_mapping.get(stars_text_value, -1)  # Default to -1 if text is not found

        return item
    
class SaveToMySQLPipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = 'Phani@1998',
            database = 'books'
        )

        self.cur = self.conn.cursor()

        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books(
            id int NOT NULL auto_increment,
            url VARCHAR(255),
            title text,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            stars INTEGER,
            category VARCHAR(255),
            description text,
            PRIMARY KEY (id)
            )
        """)

    def process_item(self, item, spider):

        ## Define insert statement
        self.cur.execute(""" insert into books (
        url,
        title,
        upc,
        product_type,
        price_excl_tax,
        price_incl_tax,
        tax,
        price,
        availability,
        num_reviews,
        stars,
        category,
        description
        ) values (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
        )""",(
        item["url"],
        item["title"],
        item["upc"],
        item["product_type"],
        item["price_excl_tax"],
        item["price_incl_tax"],
        item["tax"],
        item["price"],
        item["availability"],
        item["num_reviews"],
        item["stars"],
        item["category"],
        str(item["description"][0])
        ))

        # ## Execute insert of data into database
        self.conn.commit()
        return item
    
    def close_spider(self,spider):
        self.cur.close()
        self.conn.close()