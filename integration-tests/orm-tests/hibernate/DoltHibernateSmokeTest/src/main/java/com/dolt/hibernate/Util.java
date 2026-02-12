package com.dolt.hibernate;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

@SuppressWarnings("deprecation")
public class Util {

	private static final SessionFactory sessionFactory;

	static{
		try{
            Configuration config = new Configuration();

            config.setProperty("hibernate.connection.url",      "jdbc:" + System.getenv("DB_URL"));
            config.setProperty("hibernate.connection.username", System.getenv("DB_USER"));
            config.setProperty("hibernate.connection.password", System.getenv("DB_PASSWORD"));

			sessionFactory = config.configure().buildSessionFactory();
		}catch (Throwable ex) {
			System.err.println("Session Factory could not be created." + ex);
			throw new ExceptionInInitializerError(ex);
		}
	}

	public static SessionFactory getSessionFactory() {
		return sessionFactory;
	}

}
