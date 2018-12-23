package io.cresco.cep;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class Activator implements BundleActivator
{


    /**
     * Implements BundleActivator.start(). Prints
     * a message and adds itself to the bundle context as a service
     * listener.
     * @param context the framework context for the bundle.
     **/

    private List configurationList = new ArrayList();

    public void start(BundleContext context)
    {

        try {
/*
            startJar(context,"antlr4-runtime-4.7.jar");
            startJar(context,"esper-avro-7.1.0.jar");
*/
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    private void startJar(BundleContext context, String name) {

        try {

            String publisherBundlePath = getClass().getClassLoader().getResource(name).getPath();
            InputStream publisherBundleStream = getClass().getClassLoader().getResourceAsStream(name);
            context.installBundle(publisherBundlePath,publisherBundleStream).start();

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    /**
     * Implements BundleActivator.stop(). Prints
     * a message and removes itself from the bundle context as a
     * service listener.
     * @param context the framework context for the bundle.
     **/
    public void stop(BundleContext context)
    {
        System.out.println("Stopped Bundle.");

    }

}