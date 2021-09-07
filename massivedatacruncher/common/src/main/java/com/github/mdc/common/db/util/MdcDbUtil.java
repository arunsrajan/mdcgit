package com.github.mdc.common.db.util;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.query.Query;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.db.MdcPipelineJobResult;
import com.github.mdc.common.db.MdcResultStageExecutors;

public class MdcDbUtil {
	private MdcDbUtil() {}
	private static Logger log = Logger.getLogger(MdcDbUtil.class);
	

	@SuppressWarnings("ucd")
	
	private static void saveObject(Object obj, boolean isUpdate) {
		Transaction transaction = null;
		// Create registry
		try (StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.configure(new File(MDCProperties.get().getProperty(MDCConstants.HIBCFG))).build();) {
			// Create MetadataSources
			MetadataSources sources = new MetadataSources(registry);

			// Create Metadata
			Metadata metadata = sources.getMetadataBuilder().build();

			try (SessionFactory sessionfactory = metadata.getSessionFactoryBuilder().build();
					Session session = sessionfactory.openSession();) {
				// start a transaction
				transaction = session.beginTransaction();
				// save the pipelinejob objects
				if (isUpdate) {
					session.saveOrUpdate(obj);
				} else {
					session.save(obj);
				}

				// commit transaction
				transaction.commit();
			} catch (Exception e) {
				if (transaction != null) {
					transaction.rollback();
				}
				log.error(MDCConstants.EMPTY, e);
			}
		}catch (Exception e) {
			log.error(MDCConstants.EMPTY, e);
		}
	}

	public static List<MdcResultStageExecutors> getMdcResultStageExecutorsFromDb(String id) {
		try (StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.configure(new File(MDCProperties.get().getProperty(MDCConstants.HIBCFG))).build();) {
			// Create MetadataSources
			MetadataSources sources = new MetadataSources(registry);

			// Create Metadata
			Metadata metadata = sources.getMetadataBuilder().build();

			try (SessionFactory sessionfactory = metadata.getSessionFactoryBuilder().build();
					Session session = sessionfactory.openSession();) {
				String hql = "FROM MdcResultStageExecutors where stageId = :stageid";
				Query<?> query = session.createQuery(hql);
				query.setString("stageid", id);
				return (List<MdcResultStageExecutors>) query.list();
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY, e);
			}
		} catch (Exception e) {
			log.error(MDCConstants.EMPTY, e);
		}
		return null;
	}

	public static List<MdcResultStageExecutors> getMdcResultStageExecutorsFromDbByMdcResultId(String mdcresultid) {
		try (StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.configure(new File(MDCProperties.get().getProperty(MDCConstants.HIBCFG))).build();) {
			// Create MetadataSources
			MetadataSources sources = new MetadataSources(registry);

			// Create Metadata
			Metadata metadata = sources.getMetadataBuilder().build();

			try (SessionFactory sessionfactory = metadata.getSessionFactoryBuilder().build();
					Session session = sessionfactory.openSession()) {
				String hql = "FROM MdcResultStageExecutors where mdcResultId = :mdcresultid";
				Query<?> query = session.createQuery(hql);
				query.setString("mdcresultid", mdcresultid);
				return (List<MdcResultStageExecutors>) query.list();
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY, e);
			}
		} catch (Exception e) {
			log.error(MDCConstants.EMPTY, e);
		}
		return null;
	}

	public static List<MdcPipelineJobResult> getMdcPipelineJobResultFromDb() {
		try (StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
				.configure(new File(MDCProperties.get().getProperty(MDCConstants.HIBCFG))).build();) {
			// Create MetadataSources
			MetadataSources sources = new MetadataSources(registry);

			// Create Metadata
			Metadata metadata = sources.getMetadataBuilder().build();

			try (SessionFactory sessionfactory = metadata.getSessionFactoryBuilder().build();
					Session session = sessionfactory.openSession()) {
				String hql = "FROM MdcPipelineJobResult";
				Query<?> query = session.createQuery(hql);
				return (List<MdcPipelineJobResult>) query.list();
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY, e);
			}
		} catch (Exception e) {
			log.error(MDCConstants.EMPTY, e);
		}
		return null;
	}

	public static void saveObjectDb(MdcPipelineJobResult mdcPipelineJobResult, boolean isUpdate) {
		if (isUpdate) {
			Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
			mdcPipelineJobResult.setModifiedDate(timeStamp);
		} else {
			Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
			mdcPipelineJobResult.setCreatedDate(timeStamp);
			mdcPipelineJobResult.setModifiedDate(timeStamp);
		}
		saveObject(mdcPipelineJobResult, isUpdate);
	}

	public static void saveObjectDb(MdcResultStageExecutors mdcResultStageExecutors, boolean isUpdate) {
		if (isUpdate) {
			Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
			mdcResultStageExecutors.setStageExecutionCompletionTime(timeStamp);
		} else {
			mdcResultStageExecutors.setStageExecutionSubmittedTime(new Timestamp(System.currentTimeMillis()));
		}
		saveObject(mdcResultStageExecutors, isUpdate);
	}
}
