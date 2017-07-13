package com.xjd.bd.postgresql;

import java.sql.*;
import java.util.LinkedList;

/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.postgresql
 * Author : Xu Jiandong
 * CreateTime : 2017-06-30 14:46:00
 * ModificationHistory :
 */
public class TestPg {
    class Blog
    {
        int id;
        String source_type;
        String storage_type;
        String storage_prefix;
        String storage_suffix;
        String storage_date_format;
        Boolean is_storage_base_date;
        Boolean is_folder;
        String node;

        @Override
        public String toString() {
            return "Blog{" +
                    "id=" + id +
                    ", source_type='" + source_type + '\'' +
                    ", storage_type='" + storage_type + '\'' +
                    ", storage_prefix='" + storage_prefix + '\'' +
                    ", storage_suffix='" + storage_suffix + '\'' +
                    ", storage_date_format='" + storage_date_format + '\'' +
                    ", is_storage_base_date=" + is_storage_base_date +
                    ", is_folder=" + is_folder +
                    ", node='" + node + '\'' +
                    '}';
        }
    }

    public static void main(String[] args)
    {
        new TestPg();
    }

    private TestPg()
    {
        Connection conn = null;
        LinkedList listOfBlogs = new LinkedList();

        // connect to the database
        conn = connectToDatabaseOrDie();

        // get the data
        populateListOfTopics(conn, listOfBlogs);

        // print the results
        printTopics(listOfBlogs);
    }

    private void printTopics(LinkedList listOfBlogs)
    {
        for (Object listOfBlog : listOfBlogs) {
            Blog blog = (Blog) listOfBlog;
            System.out.println(blog.toString());
        }
    }

    private void populateListOfTopics(Connection conn, LinkedList listOfBlogs)
    {
        try
        {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT *FROM storage_status ORDER BY id");
            while ( rs.next() )
            {
                Blog blog = new Blog();
                blog.id        = rs.getInt    ("id");
                blog.source_type   = rs.getString ("source_type");
                blog.storage_type = rs.getString ("storage_type");
                blog.storage_prefix = rs.getString("storage_prefix");
                blog.storage_suffix = rs.getString("storage_suffix");
                blog.is_storage_base_date = rs.getBoolean("is_storage_base_date");
                blog.is_folder = rs.getBoolean("is_folder");
                blog.node = rs.getString("node");

                listOfBlogs.add(blog);

            }
            rs.close();
            st.close();
        }
        catch (SQLException se) {
            System.err.println("Threw a SQLException creating the list of blogs.");
            System.err.println(se.getMessage());
        }
    }

    private Connection connectToDatabaseOrDie()
    {
        Connection conn = null;
        try
        {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://192.168.101.71:6432/sh";
            conn = DriverManager.getConnection(url,"sh", "123456789!");
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
            System.exit(1);
        }
        catch (SQLException e)
        {
            e.printStackTrace();
            System.exit(2);
        }
        return conn;
    }
}
